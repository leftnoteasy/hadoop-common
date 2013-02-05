/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Overview of design
 *
 * This JNI code implements an HNP running underneath a Hadoop Map-Reduce application. It
 * looks a lot like "orterun" (aka. "mpirun"), but has the interfaces required for operation
 * as a JNI module instead of a direct command line tool. Thus, instead of looking at the
 * cmd line to get the application, or probing the RM to get the allocated nodes, we rely
 * on the MR application to pass that information down to us via the appropriate API.
 *
 * The JNI module works as follows:
 *
 * (1) the Java JobClient (or whatever is the appropriate Hadoop class) calls "initNative"
 *     somewhere during its startup - preferably as early as possible so we don't waste
 *     time trying to run when we cannot do so. This call sets up the ORTE system for
 *     operation, ensuring that all required data structures have been created.
 *
 * (2) JobClient obtains an allocation per the user's specification (e.g., taking into
 *     account file locations) and passes that information to this module via calls to
 *     the "addNode" API, one container at a time. This allows ORTE to build is allocated
 *     node array so it knows where it will be executing the actual application
 *
 * (3) JobClient passes the mapper information down to this module via the "addMapper"
 *     API. You can specify the number of mappers to run, or provide a "-1" to indicate
 *     launch one mapper on each host. Environmental variables and arguments can also be
 *     passed in the API. JobClient passes the reducer information the same way using
 *     with the "addReducer" API. You must specify the number of reducers to run - we
 *     will launch one reducer per host in a round-robin pattern across the available hosts.
 *
 * (4) Once all the executable info has been passed, the application must call the
 *     JobClient "run" API. This triggers ORTE to launch its daemons on the allocated
 *     nodes, execute the mapper, and then execute the reducer. The "run" function will
 *     return once BOTH functions (mapper and reducer) have completed.
 *
 * We can provide additional controls at a later time - e.g., to simultaneously run mappers
 * and reducers in "streaming" mode.
 */

#include "orte_config.h"
#include "orte/constants.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include <stdio.h>
#include <ctype.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#include <fcntl.h>
#include <errno.h>
#include <signal.h>

#if OPAL_WANT_LIBLTDL
  #ifndef __WINDOWS__
    #if OPAL_LIBLTDL_INTERNAL
      #include "opal/libltdl/ltdl.h"
    #else
      #include "ltdl.h"
    #endif
  #else
    #include "ltdl.h"
  #endif
#endif


#include "opal/mca/base/base.h"
#include "opal/util/basename.h"
#include "opal/util/path.h"
#include "opal/util/cmd_line.h"
#include "opal/util/output.h"
#include "opal/util/show_help.h"
#include "opal/runtime/opal.h"
#include "opal/mca/base/mca_base_param.h"
#include "opal/mca/installdirs/installdirs.h"
#include "opal/mca/event/event.h"


#include "orte/util/name_fns.h"
#include "orte/util/nidmap.h"
#include "orte/util/proc_info.h"
#include "orte/util/pre_condition_transports.h"
#include "orte/util/show_help.h"
#include "orte/mca/dfs/dfs.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/filem/filem.h"
#include "orte/mca/grpcomm/base/base.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/rmaps/base/base.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/state/state.h"
#include "orte/orted/orted.h"

#include "orte/runtime/runtime.h"
#include "orte/runtime/orte_globals.h"
#include <jni.h>

//#include "OmpiJobClient.h"

/* MRPLUS globals */
extern int mrplus_jobclient_output;

/* Internal globals */
extern opal_pointer_array_t mrplus_jobclients;
static int mrplus_index=0;

/* base object for tracking map/reduce pairs */
typedef struct {
    opal_object_t super;
    int idx;
    bool active;
    orte_job_t *mapper;
    orte_job_t *reducer;
    orte_job_t* am;
    opal_list_t map_apps;
    opal_list_t red_apps;
    orte_app_context_t* am_app;
    char **files;
    char **addedenv;
    char **cps;
    opal_pointer_array_t file_maps;
} orte_jobclient_t;

static void jc_cons(orte_jobclient_t *ptr)
{
    ptr->active = false;
    ptr->mapper = OBJ_NEW(orte_job_t);
    ptr->reducer = OBJ_NEW(orte_job_t);
    ptr->am = OBJ_NEW(orte_job_t);
    OBJ_CONSTRUCT(&ptr->map_apps, opal_list_t);
    OBJ_CONSTRUCT(&ptr->red_apps, opal_list_t);
    ptr->am_app = NULL;
    ptr->files = NULL;
    ptr->addedenv = NULL;
    ptr->cps = NULL;
    OBJ_CONSTRUCT(&ptr->file_maps, opal_pointer_array_t);
    opal_pointer_array_init(&ptr->file_maps, 8, INT_MAX, 8);
}

static void jc_des(orte_jobclient_t *ptr)
{
    opal_list_item_t *item;
    int i;
    orte_dfs_vpidfm_t *vfm;

    if (NULL != ptr->mapper) {
        OBJ_RELEASE(ptr->mapper);
    }
    if (NULL != ptr->reducer) {
        OBJ_RELEASE(ptr->reducer);
    }
    if (NULL != ptr->am) {
        OBJ_RELEASE(ptr->am);
    }
    while (NULL != (item = opal_list_remove_first(&ptr->map_apps))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ptr->map_apps);
    while (NULL != (item = opal_list_remove_first(&ptr->red_apps))) {
        OBJ_RELEASE(item);
    }
    OBJ_DESTRUCT(&ptr->red_apps);
    if (NULL != ptr->am_app) {
        OBJ_RELEASE(ptr->am_app);
    }
    if (NULL != ptr->files) {
        opal_argv_free(ptr->files);
    }
    if (NULL != ptr->addedenv) {
        opal_argv_free(ptr->addedenv);
    }
    if (NULL != ptr->cps) {
        opal_argv_free(ptr->cps);
    }
    for (i=0; i < ptr->file_maps.size; i++) {
        if (NULL != (vfm = (orte_dfs_vpidfm_t*)opal_pointer_array_get_item(&ptr->file_maps, i))) {
            OBJ_RELEASE(vfm);
        }
    }
    OBJ_DESTRUCT(&ptr->file_maps);
}
OBJ_CLASS_INSTANCE(orte_jobclient_t,
                   opal_object_t,
                   jc_cons, jc_des);

/* class for tracking apps on a prioritized list
 * so we can later insert them into their job in
 * priority order
 */
typedef struct {
    opal_list_item_t super;
    int priority;
    orte_app_context_t *app;
} orte_jobclient_app_t;
OBJ_CLASS_INSTANCE(orte_jobclient_app_t,
                   opal_list_item_t,
                   NULL, NULL);

/* local class to track operations */
typedef struct {
    opal_object_t super;
    opal_event_t ev;
    orte_jobclient_t *jc;
    orte_jobclient_app_t *client;
    int idx;
    char *data;
    bool active;
} jobclient_ops_t;
static void op_const(jobclient_ops_t *op)
{
    op->active = true;
    op->data = NULL;
}
static void op_dest(jobclient_ops_t *op)
{
    if (NULL != op->data) {
        free(op->data);
    }
}
OBJ_CLASS_INSTANCE(jobclient_ops_t,
                   opal_object_t,
                   op_const, op_dest);

#define MRPLUS_JOBCLIENT_STATE(o, f)                                    \
    do {                                                                \
        opal_event_set(orte_event_base, &(o)->ev, -1,                   \
                       OPAL_EV_WRITE, (f), (o));                        \
        opal_event_set_priority(&(o)->ev, ORTE_SYS_PRI);                \
        opal_event_active(&(o)->ev, OPAL_EV_WRITE, 1);                  \
    } while(0);


static void initNative(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;

    /* create a jobclient object for this job */
    op->jc = OBJ_NEW(orte_jobclient_t);

    /* insert it in our list, keeping the index */
    opal_pointer_array_set_item(&mrplus_jobclients, mrplus_index, op->jc);
    op->jc->idx = mrplus_index;
    mrplus_index++;
    op->active = false;
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_initNative(JNIEnv *env, jobject obj)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: initNative called");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);

    /* get the field id */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* execute the request */
    MRPLUS_JOBCLIENT_STATE(op, initNative);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }

    /* set the jobID field in this object to the index in the array so we
     * can identify a later API call to the specific instance
     * being referenced
     */
    (*env)->SetIntField(env, obj, fid, op->jc->idx);
    opal_output_verbose(2, mrplus_jobclient_output, "after initNative");
    OBJ_RELEASE(op);

    return JNI_TRUE;
}                                                   

static void addFiles(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        return;
    }

    opal_output_verbose(2, mrplus_jobclient_output,
                        "jobclient: addFiles for instance %d called with files %s",
                        op->idx, op->data);

    opal_argv_append_nosize(&jc->files, op->data);
    op->active = false;
}

/* copy files to remote execution point */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_addFiles(JNIEnv *env, jobject obj, jstring filePaths)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addFiles called");

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);
    op->data = (char*)calloc(strlen((*env)->GetStringUTFChars(env,filePaths,0)) + 1, sizeof(char));
    strcpy(op->data, (*env)->GetStringUTFChars(env,filePaths,0));

    /* execute the request */
    MRPLUS_JOBCLIENT_STATE(op, addFiles);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }

    OBJ_RELEASE(op);
}

static void addJars(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;
    char **paths;
    int i;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addJars called for instance %d", op->idx);

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        return;
    }

    /* we can't just throw the jars onto the files list as
     * we also need to set the classpath, so we need to
     * separate out the individual jars from the comma-separated list
     */
    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: got jars: %s", op->data);

    /* separate the comma-delimited list of jars into an array of
     * individual strings
     */
    paths = opal_argv_split(op->data, ',');

    /* add them to the list of file move requests */
    for (i=0; NULL != paths[i]; i++) {
        opal_output_verbose(2, mrplus_jobclient_output, "jobclient: Adding jar %d: %s\n", i, paths[i]);
        opal_argv_append_nosize(&jc->files, paths[i]);
        /* add them also to the list of classpaths */
        opal_argv_append_nosize(&jc->cps, paths[i]);
    }

    /* release the intermediate storage */
    opal_argv_free(paths);
    op->active = false;
}

/* copy jar files and them to the CLASSPATH of the app prior to execution
 * we keep them in a separate array from files just so we remember to
 * add them to the CLASSPATH
 */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_addJars(JNIEnv *env, jclass obj, jobjectArray jarPaths)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addJars called");

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);
    op->data = (char*)calloc(strlen((*env)->GetStringUTFChars(env,jarPaths,0)) + 1, sizeof(char));
    strcpy(op->data, (*env)->GetStringUTFChars(env,jarPaths,0));

    /* execute the request */
    MRPLUS_JOBCLIENT_STATE(op, addJars);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }

    OBJ_RELEASE(op);
}

static void addEnv(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addEnv called for instance %d", op->idx);

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        return;
    }

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: adding envar %s to instance %d", op->data, op->idx);

    /* add it to the array - we will massage the "key=value" format later */
    opal_argv_append_nosize(&jc->addedenv, op->data);

    op->active = false;
}

/* environ variable to add to all executables */
JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_addEnv(JNIEnv *env, jclass obj, jstring keyval)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);
    op->data = (char*)calloc(strlen((*env)->GetStringUTFChars(env,keyval,0)) + 1, sizeof(char));
    strcpy(op->data, (*env)->GetStringUTFChars(env,keyval,0));

    /* execute the request */
    MRPLUS_JOBCLIENT_STATE(op, addEnv);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }

    OBJ_RELEASE(op);
}

static void addAppMaster(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addAppMaster called");

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        return;
    }

    /* add env in jc to app */
    op->client->app->env = jc->addedenv;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addAppMaster called for instance %d", op->idx);

    /* add app_context to jc, first check if the jc already have am_app */
    if (NULL != jc->am_app) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        return;
    }
    jc->am_app = op->client->app;

    op->active = false;
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_addAppMaster(JNIEnv *env, jclass obj, jobjectArray args,
                                                                                      jobjectArray locations, jlong length)
{
    int i;
    orte_app_context_t *app;
    jstring string;
    int stringCount;
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);

    /* create an app tracker */
    op->client = OBJ_NEW(orte_jobclient_app_t);

    /* create a mapper app_context */
    app = OBJ_NEW(orte_app_context_t);
    op->client->app = app;

    /* add the argv */
    stringCount = (*env)->GetArrayLength(env, args);

    for (i=0; i < stringCount; i++) {
        string = (jstring) (*env)->GetObjectArrayElement(env, args, i);
        opal_output_verbose(2, mrplus_jobclient_output, "jobclient: adding argv[%d]: %s", i,
                            (*env)->GetStringUTFChars(env, string, 0));
        opal_argv_append_nosize(&app->argv, (*env)->GetStringUTFChars(env, string, 0));
    }

    /* set the app name from the first argv */
    app->app = strdup(app->argv[0]);

    /* indicate we want only one of these launched as the Java
     * code is going to pass a unique cmd line for each process
     */
    app->num_procs = 1;

    /* we will always use the proc session dir as our cwd */
    app->set_cwd_to_session_dir = true;

    /* execute the request */
    MRPLUS_JOBCLIENT_STATE(op, addAppMaster);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }

    OBJ_RELEASE(op);

    return JNI_TRUE;
}

static void addMapper(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;
    bool found;
    opal_list_item_t *item;
    orte_jobclient_app_t *cl;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addMapper called");

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        return;
    }

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addMapper called for instance %d", op->idx);


    /* add it to the mapper app list in priority order */
    found = false;
    for (item = opal_list_get_first(&jc->map_apps);
         item != opal_list_get_end(&jc->map_apps);
         item = opal_list_get_next(item)) {
        cl = (orte_jobclient_app_t*)item;
        if (cl->priority < op->client->priority) {
            opal_list_insert_pos(&jc->map_apps, item, &op->client->super);
            found = true;
            break;
        }
    }
    if (!found) {
        /* append to end */
        opal_list_append(&jc->map_apps, &op->client->super);
    }

    op->active = false;
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_addMapper(JNIEnv *env, jclass obj, jobjectArray args,
                                                                                      jobjectArray locations, jlong length)
{
    int i;
    orte_app_context_t *app;
    jstring string;
    int stringCount;
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);

    /* create an app tracker */
    op->client = OBJ_NEW(orte_jobclient_app_t);

    /* create a mapper app_context */
    app = OBJ_NEW(orte_app_context_t);
    op->client->app = app;

    /* add the argv */
    stringCount = (*env)->GetArrayLength(env, args);

    for (i=0; i < stringCount; i++) {
        string = (jstring) (*env)->GetObjectArrayElement(env, args, i);
        opal_output_verbose(2, mrplus_jobclient_output, "jobclient: adding argv[%d]: %s", i,
                            (*env)->GetStringUTFChars(env, string, 0));
        opal_argv_append_nosize(&app->argv, (*env)->GetStringUTFChars(env, string, 0));
    }

    /* set the app name from the first argv */
    app->app = strdup(app->argv[0]);

    /* indicate we want only one of these launched as the Java
     * code is going to pass a unique cmd line for each process
     */
    app->num_procs = 1;

    /* save the hint location in the -hosts field */
    if (NULL != locations) {
        stringCount = (*env)->GetArrayLength(env, locations);

        for (i=0; i < stringCount; i++) {
            string = (jstring) (*env)->GetObjectArrayElement(env, locations, i);
            opal_output_verbose(2, mrplus_jobclient_output, "jobclient: adding locations[%d]: %s", i,
                                (*env)->GetStringUTFChars(env, string, 0));
            opal_argv_append_nosize(&app->dash_host, (*env)->GetStringUTFChars(env, string, 0));
        }
    }

    /* save the length hint */
    op->client->priority = length;

    /* we will always use the proc session dir as our cwd */
    app->set_cwd_to_session_dir = true;

    /* execute the request */
    MRPLUS_JOBCLIENT_STATE(op, addMapper);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }

    OBJ_RELEASE(op);

    return JNI_TRUE;
}

static void addReducer(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;
    bool found;
    opal_list_item_t *item;
    orte_jobclient_app_t *cl;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addReducer called");

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        return;
    }

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: addReducer called for instance %d", op->idx);


    /* add it to the reducer app list in priority order */
    found = false;
    for (item = opal_list_get_first(&jc->red_apps);
         item != opal_list_get_end(&jc->red_apps);
         item = opal_list_get_next(item)) {
        cl = (orte_jobclient_app_t*)item;
        if (cl->priority < op->client->priority) {
            opal_list_insert_pos(&jc->red_apps, item, &op->client->super);
            found = true;
            break;
        }
    }
    if (!found) {
        /* append to end */
        opal_list_append(&jc->red_apps, &op->client->super);
    }

    op->active = false;
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_addReducer(JNIEnv *env, jclass obj, jobjectArray args,
                                                                                      jobjectArray locations, jlong length)
{
    int i;
    orte_app_context_t *app;
    jstring string;
    int stringCount;
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient:  called");

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);

    /* create an app tracker */
    op->client = OBJ_NEW(orte_jobclient_app_t);

    /* create a reducer app_context */
    app = OBJ_NEW(orte_app_context_t);
    op->client->app = app;

    /* add the argv */
    stringCount = (*env)->GetArrayLength(env, args);

    for (i=0; i < stringCount; i++) {
        string = (jstring) (*env)->GetObjectArrayElement(env, args, i);
        opal_output_verbose(2, mrplus_jobclient_output, "jobclient: adding argv[%d]: %s", i,
                            (*env)->GetStringUTFChars(env, string, 0));
        opal_argv_append_nosize(&app->argv, (*env)->GetStringUTFChars(env, string, 0));
    }

    /* set the app name from the first argv */
    app->app = strdup(app->argv[0]);

    /* indicate we want only one of these launched as the Java
     * code is going to pass a unique cmd line for each process
     */
    app->num_procs = 1;

    /* save the hint location in the -hosts field */
    if (NULL != locations) {
        stringCount = (*env)->GetArrayLength(env, locations);

        for (i=0; i < stringCount; i++) {
            string = (jstring) (*env)->GetObjectArrayElement(env, locations, i);
            opal_output_verbose(2, mrplus_jobclient_output, "jobclient: adding locations[%d]: %s", i,
                                (*env)->GetStringUTFChars(env, string, 0));
            opal_argv_append_nosize(&app->dash_host, (*env)->GetStringUTFChars(env, string, 0));
        }
    }

    /* save the length hint */
    op->client->priority = length;

    /* we will always use the proc session dir as our cwd */
    app->set_cwd_to_session_dir = true;

    /* execute the request */
    MRPLUS_JOBCLIENT_STATE(op, addReducer);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }

    OBJ_RELEASE(op);

    return JNI_TRUE;
}

static void spawn_reducer(opal_buffer_t *buffer, void *cbdata)
{
    orte_jobclient_t *jc = (orte_jobclient_t*)cbdata;
    char *cpadd=NULL, *cpfinal;
    bool inserted;
    orte_jobclient_app_t *client;
    char *maxprocs;
    int j;
    opal_buffer_t *xfer, xfr2, *bptr;
    orte_vpid_t vpid;
    int32_t i, k, n, nvpids;
    int rc, cnt;
    orte_dfs_vpidfm_t *vfm;
    int64_t i64;
    int32_t ncnt;

    /* if file maps were found, then we need to shuffle them
     * so that each reducer can get the maps flagged for it
     */
    if (NULL != buffer) {
        ncnt = 0;
        /* retrieve the number of vpids in the map */
        cnt = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &nvpids, &cnt, OPAL_INT32))) {
            ORTE_ERROR_LOG(rc);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return;
        }
        opal_output_verbose(1, mrplus_jobclient_output,
                            "%s RECVD DATA FROM %d VPIDS",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            nvpids);

        for (k=0; k < nvpids; k++) {
            /* get the vpid for this map */
            cnt = 1;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &vpid, &cnt, ORTE_VPID))) {
                ORTE_ERROR_LOG(rc);
                ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                return;
            }
            opal_output_verbose(1, mrplus_jobclient_output,
                                "%s RECEIVED ENTRIES FROM MAPPER %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_VPID_PRINT(vpid));
            /* get number of entries for this vpid */
            cnt = 1;
            if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &n, &cnt, OPAL_INT32))) {
                ORTE_ERROR_LOG(rc);
                ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                return;
            }
            opal_output_verbose(1, mrplus_jobclient_output,
                                "%s\tRECVD %d ENTRIES",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), n);
            /* loop thru the entries and transfer them across */
            cnt = 1;
            for (i=0; i < n; i++) {
                opal_output_verbose(1, mrplus_jobclient_output,
                                    "%s\tWORKING ENTRY %d",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i);
                /* unpack the buffer containing this entry */
                cnt = 1;
                if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &xfer, &cnt, OPAL_BUFFER))) {
                    ORTE_ERROR_LOG(rc);
                    break;
                }
                /* get the entry for this partition */
                cnt = 1;
                if (OPAL_SUCCESS != (rc = opal_dss.unpack(xfer, &i64, &cnt, OPAL_INT64))) {
                    ORTE_ERROR_LOG(rc);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return;
                }
                /* find the tracker for it */
                if (NULL == (vfm = (orte_dfs_vpidfm_t*)opal_pointer_array_get_item(&jc->file_maps, i64))) {
                    vfm = OBJ_NEW(orte_dfs_vpidfm_t);
                    vfm->vpid = i64;
                    opal_pointer_array_set_item(&jc->file_maps, i64, vfm);
                    ncnt++;
                }
                ++vfm->num_entries;
                OBJ_CONSTRUCT(&xfr2, opal_buffer_t);
                /* mark the source of the data */
                if (OPAL_SUCCESS != (rc = opal_dss.pack(&xfr2, &vpid, 1, ORTE_VPID))) {
                    ORTE_ERROR_LOG(rc);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return;
                }
                /* add the remaining data in the buffer to it */
                if (OPAL_SUCCESS != (rc = opal_dss.pack(&xfr2, &xfer, 1, OPAL_BUFFER))) {
                    ORTE_ERROR_LOG(rc);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return;
                }
                /* now make that the next entry */
                bptr = &xfr2;
                if (OPAL_SUCCESS != (rc = opal_dss.pack(&vfm->data, &bptr, 1, OPAL_BUFFER))) {
                    ORTE_ERROR_LOG(rc);
                    ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                    return;
                }
                OBJ_DESTRUCT(&xfr2);
                OBJ_RELEASE(xfer);
            }
        }
        /* now assemble the map for the reducer job - note that we
         * cannot use orte_dfs.get_file_maps because the reducer job
         * has not yet been assigned a jobid!
         */
        if (NULL != jc->reducer->file_maps) {
            OBJ_RELEASE(jc->reducer->file_maps);
        }
        jc->reducer->file_maps = OBJ_NEW(opal_buffer_t);
        /* indicate the number of vpids in this new map */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(jc->reducer->file_maps, &ncnt, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(rc);
                ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                return;
            }
        for (i=0; i < jc->file_maps.size; i++) {
            if (NULL == (vfm = (orte_dfs_vpidfm_t*)opal_pointer_array_get_item(&jc->file_maps, i))) {
                continue;
            }
            /* indicate data for this vpid */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(jc->reducer->file_maps, &vfm->vpid, 1, ORTE_VPID))) {
                ORTE_ERROR_LOG(rc);
                ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                return;
            }
            /* pack the number of entries for it */
            if (OPAL_SUCCESS != (rc = opal_dss.pack(jc->reducer->file_maps, &vfm->num_entries, 1, OPAL_INT32))) {
                ORTE_ERROR_LOG(rc);
                ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
                ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
                return;
            }
            /* transfer the data */
            opal_dss.copy_payload(jc->reducer->file_maps, &vfm->data);
        }
    }

    /* if classpaths were provided, setup to add them */
    if (NULL != jc->cps) {
        cpadd = opal_argv_join(jc->cps, ':');
    }

    while (NULL != (client = (orte_jobclient_app_t*)opal_list_remove_first(&jc->red_apps))) {
        /* add the app to the reducer job */
        client->app->idx = opal_pointer_array_add(jc->reducer->apps, client->app);
        jc->reducer->num_apps++;
        /* check for specification of max procs/node for the reducer */
        if (NULL != (maxprocs = getenv("MRPLUS_MAX_REDUCER_PROCS_PER_NODE"))) {
            client->app->max_procs_per_node = strtol(maxprocs, NULL, 10);
        }
        /* add the files to be prepositioned */
        if (NULL != jc->files) {
            client->app->preload_files = opal_argv_join(jc->files, ',');
        }
        /* add any classpaths to the cmd line of the app */
        if (NULL != cpadd) {
            inserted = false;
            for (j=0; NULL != client->app->argv[j]; j++) {
                if (0 == strcmp(client->app->argv[j], "-cp") ||
                    0 == strcmp(client->app->argv[j], "-classpath")) {
                    /* the next argument is the specified classpath - append ours */
                    asprintf(&cpfinal, "%s:%s", client->app->argv[j+1], cpadd);
                    free(client->app->argv[j+1]);
                    client->app->argv[j+1] = cpfinal;
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                /* must add the option */
                opal_argv_append_nosize(&client->app->argv, "-cp");
                opal_argv_append_nosize(&client->app->argv, cpadd);
            }
        }
        /* dump the tracker - won't do anything to the app_context
         * it carries
         */
        OBJ_RELEASE(client);
    }
    if (NULL != cpadd) {
        free(cpadd);
    }

    /* pre-condition any network transports that require it */
    if (ORTE_SUCCESS != orte_pre_condition_transports(jc->reducer)) {
        opal_output(0, "PRECONDITION TRANSPORTS FAILED");
        ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
        ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        return;
    }

    opal_output_verbose(2, mrplus_jobclient_output, "JOBCLIENT: running reducer for instance %d", jc->idx);

    /* spawn the reducer job */
    if (ORTE_SUCCESS != orte_plm.spawn(jc->reducer)) {
        opal_output(0, "REDUCER FAILED TO SPAWN");
        ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
    }
}

static void runReducer(int fd, short sd, void *cbdata)
{
    orte_state_caddy_t *caddy = (orte_state_caddy_t*)cbdata;
    int idx;
    bool reducer=false;
    orte_jobclient_t *jc, *jptr;
    orte_process_name_t target;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: runReducer called");

    /* get the jobclient object for this instance */
    jc = NULL;
    for (idx=0; idx < mrplus_jobclients.size; idx++) {
        if (NULL == (jptr = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, idx))) {
            continue;
        }
        if (NULL != jptr->mapper && caddy->jdata->jobid == jptr->mapper->jobid) {
            jc = jptr;
            break;
        }
        if (NULL != jptr->reducer && caddy->jdata->jobid == jptr->reducer->jobid) {
            jc = jptr;
            reducer = true;
            break;
        }
    }
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        ORTE_TERMINATE(ORTE_ERR_NOT_FOUND);
        OBJ_RELEASE(caddy);
        return;
    }

    if (reducer) {
        opal_output_verbose(2, mrplus_jobclient_output, "JOBCLIENT: reducer complete for instance %d", idx);
        /* if the reducer is the job that finished, then we want runJob to return */
        jc->active = false;
        /* cleanup */
        if (NULL != jc->reducer &&
            NULL != opal_pointer_array_get_item(orte_job_data, ORTE_LOCAL_JOBID(jc->reducer->jobid))) {
            opal_output_verbose(2, mrplus_jobclient_output, "jobclient: cleaning reducer");
            opal_pointer_array_set_item(orte_job_data, ORTE_LOCAL_JOBID(jc->reducer->jobid), NULL);
        }
        if (NULL != jc->mapper &&
            NULL != opal_pointer_array_get_item(orte_job_data, ORTE_LOCAL_JOBID(jc->mapper->jobid))) {
            opal_output_verbose(2, mrplus_jobclient_output, "jobclient: cleaning mapper");
            opal_pointer_array_set_item(orte_job_data, ORTE_LOCAL_JOBID(jc->mapper->jobid), NULL);
        }
        /* all done, so return */
        OBJ_RELEASE(caddy);
        return;
    }

    opal_output_verbose(2, mrplus_jobclient_output, "JOBCLIENT: mapper complete for instance %d", idx);

    /* since it was the mapper that completed, then launch the
     * reducer if one was given - otherwise, just ignore
     */
    if (0 == opal_list_get_size(&jc->red_apps)) {
        opal_output_verbose(2, mrplus_jobclient_output, "JOBCLIENT: no reducer for instance %d", idx);
        jc->active = false;
        /* cleanup and return */
        if (NULL != jc->mapper &&
            NULL != opal_pointer_array_get_item(orte_job_data, ORTE_LOCAL_JOBID(jc->mapper->jobid))) {
            opal_output_verbose(2, mrplus_jobclient_output, "jobclient: cleaning mapper");
            opal_pointer_array_set_item(orte_job_data, ORTE_LOCAL_JOBID(jc->mapper->jobid), NULL);
        }
        OBJ_RELEASE(caddy);
        return;
    }

    /* request any file maps from the mapper and send us to a callback
     * that will complete the spawn of the reducer
     */
    target.jobid = jc->mapper->jobid;
    target.vpid = ORTE_VPID_WILDCARD;
    orte_dfs.get_file_map(&target, spawn_reducer, jc);

    /* remove the mapper job */
    opal_pointer_array_set_item(orte_job_data, ORTE_LOCAL_JOBID(jc->mapper->jobid), NULL);

    /* cleanup */
    OBJ_RELEASE(caddy);
}

static void runJob(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;
    int rc, j;
    char *cpadd, *cpfinal;
    orte_jobclient_app_t *client;
    bool inserted;
    char *maxprocs;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: runJob called");

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        op->idx = -1;
        return;
    }

    /* setup to run the mappers first - define a state machine position
     * that is fired when the mapper job completes so we can then start
     * the reducer job
     */
    if (ORTE_SUCCESS != (rc = orte_state.set_job_state_callback(ORTE_JOB_STATE_NOTIFY_COMPLETED, runReducer))) {
        ORTE_ERROR_LOG(rc);
        op->active = false;
        op->idx = -1;
        return;
    }

    /* if classpaths were provided, setup to add them */
    if (NULL != jc->cps) {
        cpadd = opal_argv_join(jc->cps, ':');
    }

    /* setup the mapper job */
    while (NULL != (client = (orte_jobclient_app_t*)opal_list_remove_first(&jc->map_apps))) {
        /* add the app to the mapper job */
        client->app->idx = opal_pointer_array_add(jc->mapper->apps, client->app);
        jc->mapper->num_apps++;
        /* check for specification of max procs/node for the reducer */
        if (NULL != (maxprocs = getenv("MRPLUS_MAX_MAPPER_PROCS_PER_NODE"))) {
            client->app->max_procs_per_node = strtol(maxprocs, NULL, 10);
        }
        /* add the files to be prepositioned */
        if (NULL != jc->files) {
            client->app->preload_files = opal_argv_join(jc->files, ',');
        }
        /* add any classpaths to the cmd line of the app */
        if (NULL != cpadd) {
            inserted = false;
            for (j=0; NULL != client->app->argv[j]; j++) {
                if (0 == strcmp(client->app->argv[j], "-cp") ||
                    0 == strcmp(client->app->argv[j], "-classpath")) {
                    /* the next argument is the specified classpath - append ours */
                    asprintf(&cpfinal, "%s:%s", client->app->argv[j+1], cpadd);
                    free(client->app->argv[j+1]);
                    client->app->argv[j+1] = cpfinal;
                    inserted = true;
                    break;
                }
            }
            if (!inserted) {
                /* must add the option */
                opal_argv_append_nosize(&client->app->argv, "-cp");
                opal_argv_append_nosize(&client->app->argv, cpadd);
            }
        }
        /* dump the tracker - won't do anything to the app_context
         * it carries
         */
        OBJ_RELEASE(client);
    }
    if (NULL != cpadd) {
        free(cpadd);
    }

    /* pre-condition any network transports that require it */
    if (ORTE_SUCCESS != orte_pre_condition_transports(jc->mapper)) {
        opal_output(0, "PRECONDITION TRANSPORTS FAILED");
        op->active = false;
        op->idx = -1;
        return;
    }

    /* mark the job as active */
    jc->active = true;

    /* pass it back */
    op->jc = jc;

    /* spawn the mapper job */
    if (ORTE_SUCCESS != orte_plm.spawn(jc->mapper)) {
        opal_output(0, "MAPPER FAILED TO SPAWN");
        op->active = false;
        op->idx = -1;
        return;
    }
    op->active = false;
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_runJob(JNIEnv *env, jclass obj)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;
    orte_jobclient_t *jc;

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);

    /* execute the spawn */
    MRPLUS_JOBCLIENT_STATE(op, runJob);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }
    if (op->idx < 0) {
        OBJ_RELEASE(op);
        return JNI_FALSE;
    }
    jc = op->jc;
    OBJ_RELEASE(op);

    /* wait until the job is complete */
    while (jc->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }
    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: runJob instance %d complete", jc->idx);

    opal_pointer_array_set_item(&mrplus_jobclients, jc->idx, NULL);
    /* don't cleanup for now - need to check that we have
     * accurate matching of reference counts in objects */
#if 0
    OBJ_RELEASE(jc);
#endif

    return JNI_TRUE;
}


static void runAM(int fd, short args, void *cbdata)
{
    jobclient_ops_t *op = (jobclient_ops_t*)cbdata;
    orte_jobclient_t *jc;
    int j;
    char *cpadd, *cpfinal;
    bool inserted;

    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: runAM called");

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, op->idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        op->active = false;
        op->idx = -1;
        return;
    }

    /* if classpaths were provided, setup to add them */
    if (NULL != jc->cps) {
        cpadd = opal_argv_join(jc->cps, ':');
    }

    opal_pointer_array_add(jc->am->apps, jc->am_app);
    jc->am->num_apps++;
    jc->am_app->preload_files = opal_argv_join(jc->files, ',');

    /* pre-condition any network transports that require it */
    if (ORTE_SUCCESS != orte_pre_condition_transports(jc->am)) {
        opal_output(0, "PRECONDITION TRANSPORTS FAILED");
        op->active = false;
        op->idx = -1;
        return;
    }

    /* mark the job as active */
    jc->active = true;

    /* pass it back */
    op->jc = jc;

    /* spawn the mapper job */
    if (ORTE_SUCCESS != orte_plm.spawn(jc->am)) {
        opal_output(0, "APPMASTER FAILED TO SPAWN");
        op->active = false;
        op->idx = -1;
        return;
    }
    op->active = false;
}

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_runAM(JNIEnv *env, jclass obj)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    jobclient_ops_t *op;
    orte_jobclient_t *jc;

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");

    /* setup the operation */
    op = OBJ_NEW(jobclient_ops_t);
    op->idx = (*env)->GetIntField(env, obj, fid);

    /* execute the spawn */
    MRPLUS_JOBCLIENT_STATE(op, runAM);
    while (op->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }
    if (op->idx < 0) {
        OBJ_RELEASE(op);
        return JNI_FALSE;
    }
    jc = op->jc;
    OBJ_RELEASE(op);

    /* wait until the job is complete */
    while (jc->active) {
        /* provide a very short quiet period so we 
         * don't hammer the cpu while we wait
         */
        struct timespec tp = {0, 100};
        nanosleep(&tp, NULL);
    }
    opal_output_verbose(2, mrplus_jobclient_output, "jobclient: runAM instance %d complete", jc->idx);

    opal_pointer_array_set_item(&mrplus_jobclients, jc->idx, NULL);
    /* don't cleanup for now - need to check that we have
     * accurate matching of reference counts in objects */
#if 0
    OBJ_RELEASE(jc);
#endif

    return JNI_TRUE;
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getActiveTrackerNames(JNIEnv *env, jclass obj)
{
    jobjectArray ret=NULL;
    int i;
    orte_node_t *node;

    /* the number of nodes in our allocation is just the
     * number of daemons in our job
     */
    ret= (jobjectArray)(*env)->NewObjectArray(env, orte_process_info.num_procs,
                                              (*env)->FindClass(env, "java/lang/String"),  
                                              (*env)->NewStringUTF(env, ""));  
   
    /* pass back the names of the nodes */
    for (i=0; i < orte_node_pool->size; i++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
            continue;
        }
        if (NULL == node->daemon) {
            continue;
        }
        (*env)->SetObjectArrayElement(env,ret,0,(*env)->NewStringUTF(env, node->name));
    }

    return(ret);  
}

JNIEXPORT jobjectArray JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getBlacklistedTrackerNames(JNIEnv *env, jclass obj)
{
    /* meaningless at the moment as all nodes in our allocation
     * are assumed available - in the future, this will change as
     * we monitor node failures
     */
    return NULL;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getBlacklistedTrackers(JNIEnv *env, jclass obj)
{
    /* meaningless at the moment as all nodes in our allocation
     * are assumed available - in the future, this will change as
     * we monitor node failures
     */
    return 0;
}

/* no good way I know of to get the Hadoop job state constants,
 * so just hand code them here
 */
#define ORTE_JOBCLIENT_INITIALIZING 0
#define ORTE_JOBCLIENT_RUNNING      1

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getJobTrackerState(JNIEnv *env, jclass obj)
{
    orte_job_t *jdata;
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    orte_jobclient_t *jc;
    jint idx;

    /* our state is always running, so it is probably meaningless to
     * return it. Instead, find the currently running job and return
     * its state
     */

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");
    idx = (*env)->GetIntField(env, obj, fid);

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, idx);
    if (NULL == jc) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_JOBCLIENT_INITIALIZING;
    }

    if (NULL != jc->mapper) {
        jdata = jc->mapper;
    } else if (NULL != jc->reducer) {
        jdata = jc->reducer;
    } else {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_JOBCLIENT_INITIALIZING;
    }

    if (jdata->state < ORTE_JOB_STATE_RUNNING) {
        return ORTE_JOBCLIENT_INITIALIZING;
    }
    return ORTE_JOBCLIENT_RUNNING;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getMapTasks(JNIEnv *env, jclass obj)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    orte_jobclient_t *jc;
    jint idx;

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");
    idx = (*env)->GetIntField(env, obj, fid);

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, idx);
    if (NULL == jc || NULL == jc->mapper) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_JOBCLIENT_INITIALIZING;
    }

    /* return the number of processes in the mapper job */
    return jc->mapper->num_procs;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getMaxMapTasks(JNIEnv *env, jclass obj)
{
    int i, nslots=0;
    orte_node_t *node;

    /* return the number of slots in our allocation */
    for (i=0; i < orte_node_pool->size; i++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
            continue;
        }
        nslots += node->slots;
    }
    return nslots;
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getMaxMemory(JNIEnv *env, jclass obj)
{
    hwloc_uint64_t min_mem = 0;
    int i;
    hwloc_topology_t topo;
    hwloc_obj_t root;

    /* scan across our node topologies and find the minimum memory
     * on a node - this is the max that any user can expect to
     * have regardless of location
     */
    for (i=0; i < orte_node_topologies->size; i++) {
        if (NULL == (topo = (hwloc_topology_t)opal_pointer_array_get_item(orte_node_topologies, i))) {
            continue;
        }
        /* get the root object */
        root = hwloc_get_root_obj(topo);

        if (root->memory.total_memory < min_mem || 0 == min_mem) {
            min_mem = root->memory.total_memory;
        }
    }

    /* the value is returned in BYTES */
    return min_mem;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getMaxReduceTasks(JNIEnv *env, jclass obj)
{
    int i, nslots=0;
    orte_node_t *node;

    /* return the number of slots in our allocation */
    for (i=0; i < orte_node_pool->size; i++) {
        if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(orte_node_pool, i))) {
            continue;
        }
        nslots += node->slots;
    }
    return nslots;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getReduceTasks(JNIEnv *env, jclass obj)
{
    jclass cls = (*env)->GetObjectClass(env, obj);
    jfieldID fid;
    orte_jobclient_t *jc;
    jint idx;

    /* get the field id for this instance */
    fid = (*env)->GetFieldID(env, cls, "jobID", "I");
    idx = (*env)->GetIntField(env, obj, fid);

    /* get the jobclient object for this instance */
    jc = (orte_jobclient_t*)opal_pointer_array_get_item(&mrplus_jobclients, idx);
    if (NULL == jc || NULL == jc->reducer) {
        /* something is wrong */
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        return ORTE_JOBCLIENT_INITIALIZING;
    }

    /* return the number of processes in the reducer job */
    return jc->reducer->num_procs;
}

JNIEXPORT jint JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getTaskTrackers(JNIEnv *env, jclass obj)
{
    /* the number of nodes in our allocation is just the
     * number of daemons in our job
     */
    return orte_process_info.num_procs;
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiJobClient_getUsedMemory(JNIEnv *env, jclass obj)
{
    /* this is a MUCH tougher request to fulfill as it requires a point-in-time
     * measurement across all nodes - doable, but not info we have readily available,
     * so just return 0 for now
     */
    return 0;
}
