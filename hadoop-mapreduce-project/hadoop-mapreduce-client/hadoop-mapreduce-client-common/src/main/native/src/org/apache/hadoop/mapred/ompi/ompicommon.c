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

#include "opal/class/opal_pointer_array.h"
#include "opal/runtime/opal.h"
#include "opal/util/basename.h"
#include "opal/util/opal_environ.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/state/state.h"
#include "orte/orted/orted.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/runtime/orte_globals.h"
#include "orte/runtime/runtime.h"
#include <jni.h>

/* MRPLUS globals */
int mrplus_filesystem_output = -1;
int mrplus_jobclient_output = -1;
int mrplus_mapred_output = -1;
opal_pointer_array_t mrplus_jobclients;

static void hostname_aliases(int status, orte_process_name_t* sender,
                             opal_buffer_t* buffer, orte_rml_tag_t tag,
                             void* cbdata);

JNIEXPORT jboolean JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiCommon_initNative(JNIEnv *env, jobject obj, jint flag)
{
    int fakeargc=0;
    char **fakeargv=NULL;
    char **argv=NULL;
    int argc=1;
    int verbosity;
    orte_proc_type_t proctype;
    int rc;
    /* first, load the required ORTE library */
#if OPAL_WANT_LIBLTDL
    lt_dladvise advise;

    if (lt_dlinit() != 0) {
        fprintf(stderr, "LT_DLINIT FAILED - CANNOT LOAD LIBMRPLUS\n");
        return JNI_FALSE;
    }

#if OPAL_HAVE_LTDL_ADVISE
    /* open the library into the global namespace */
    if (lt_dladvise_init(&advise)) {
        fprintf(stderr, "LT_DLADVISE INIT FAILED - CANNOT LOAD LIBMRPLUS\n");
        return JNI_FALSE;
    }

    if (lt_dladvise_ext(&advise)) {
        fprintf(stderr, "LT_DLADVISE EXT FAILED - CANNOT LOAD LIBMRPLUS\n");
        lt_dladvise_destroy(&advise);
        return JNI_FALSE;
    }

    if (lt_dladvise_global(&advise)) {
        fprintf(stderr, "LT_DLADVISE GLOBAL FAILED - CANNOT LOAD LIBMRPLUS\n");
        lt_dladvise_destroy(&advise);
        return JNI_FALSE;
    }

    /* we don't care about the return value
     * on dlopen - it might return an error
     * because the lib is already loaded,
     * depending on the way we were built
     */
    lt_dlopenadvise("libopen-rte", advise);
    lt_dladvise_destroy(&advise);
#else
    fprintf(stderr, "NO LT_DLADVISE - CANNOT LOAD LIBMRPLUS\n");
    /* need to balance the ltdl inits */
    lt_dlexit();
    /* if we don't have advise, then we are hosed */
    return JNI_FALSE;
#endif
#endif
    /* if dlopen was disabled, then all symbols
     * should have been pulled up into the libraries,
     * so we don't need to do anything as the symbols
     * are already available.
     */

    /* Need to initialize OPAL so that install_dirs are filled in */
    if (OPAL_SUCCESS != opal_init_util(&argc, &argv)) {
        fprintf(stderr, "Could not initialize OPAL utilities\n");
        return JNI_FALSE;
    }

    /* initialize the ORTE system */
    if (0 == flag) {
        proctype = ORTE_PROC_HNP;
    } else {
        proctype = ORTE_PROC_NON_MPI;
    }
    if (ORTE_SUCCESS != orte_init(&fakeargc, &fakeargv, proctype)) {
        fprintf(stderr, "Failed to initialize ORTE\n");
        return JNI_FALSE;
    }

    /* setup debug channels */
    mca_base_param_reg_int_name("mrplus", "filesystem_verbose",
                                "Verbosity of the MR+ filesystem subsystem",
                                true, false, 0,  &verbosity);
    if (0 < verbosity) {
        mrplus_filesystem_output = opal_output_open(NULL);
        opal_output_set_verbosity(mrplus_filesystem_output, verbosity);
    }
    mca_base_param_reg_int_name("mrplus", "jobclient_verbose",
                                "Verbosity of the MR+ jobclient subsystem",
                                true, false, 0,  &verbosity);
    if (0 < verbosity) {
        mrplus_jobclient_output = opal_output_open(NULL);
        opal_output_set_verbosity(mrplus_jobclient_output, verbosity);
    }
    mca_base_param_reg_int_name("mrplus", "mapred_verbose",
                                "Verbosity of the MR+ mapred subsystem",
                                true, false, 0,  &verbosity);
    if (0 < verbosity) {
        mrplus_mapred_output = opal_output_open(NULL);
        opal_output_set_verbosity(mrplus_mapred_output, verbosity);
    }

    if (0 == flag) {
        /* complete setup of HNP :
         * save the environment for launch purposes. This MUST be
         * done so that we can pass it to any local procs we
         * spawn - otherwise, those local procs won't see any
         * non-MCA envars were set in the enviro prior to calling
         * orterun
         */
        orte_launch_environ = opal_argv_copy(environ);
    
        /* Change the default behavior of libevent such that we want to
           continually block rather than blocking for the default timeout
           and then looping around the progress engine again.  There
           should be nothing in the orted that cannot block in libevent
           until "something" happens (i.e., there's no need to keep
           cycling through progress because the only things that should
           happen will happen in libevent).  This is a minor optimization,
           but what the heck... :-) */
        opal_progress_set_event_flag(OPAL_EVLOOP_ONCE);
        
        /* purge any ess flag set externally */
        opal_unsetenv("OMPI_MCA_ess", &orte_launch_environ);

        /* for error message purposes, assign a name to orte_basename */
        orte_basename = strdup("jobclient");

        /* setup to listen for commands sent specifically to me, even though I would probably
         * be the one sending them! Unfortunately, since I am a participating daemon,
         * there are times I need to send a command to "all daemons", and that means *I* have
         * to receive it too
         */
        rc = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_DAEMON,
                                     ORTE_RML_PERSISTENT, orte_daemon_recv, NULL);
        if (rc != ORTE_SUCCESS) {
            ORTE_ERROR_LOG(rc);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return JNI_FALSE;
        }
    
        OBJ_CONSTRUCT(&mrplus_jobclients, opal_pointer_array_t);
        opal_pointer_array_init(&mrplus_jobclients, 1, INT_MAX, 1);

        /* setup to listen for MapperOutput postings */
        rc = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_MAX+1,
                                     ORTE_RML_PERSISTENT, orte_daemon_recv, NULL);
        if (rc != ORTE_SUCCESS) {
            ORTE_ERROR_LOG(rc);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return JNI_FALSE;
        }

        /* setup to listen for hostname aliases */
        rc = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_MAX+2,
                                     ORTE_RML_PERSISTENT, hostname_aliases, NULL);
        if (rc != ORTE_SUCCESS) {
            ORTE_ERROR_LOG(rc);
            ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            return JNI_FALSE;
        }
    }

    return JNI_TRUE;
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiCommon_finiNative(JNIEnv *env, jclass obj)
{
    /* shutdown the opened parts of ORTE */
    orte_finalize();
}

JNIEXPORT void JNICALL Java_org_apache_hadoop_mapred_ompi_OmpiCommon_passAlias(JNIEnv *env, jclass obj, jstring aliasString)
{
    char *alias;
    opal_buffer_t *buf;
    int rc;

    if (NULL != aliasString) {
        alias = (char*)(*env)->GetStringUTFChars(env,aliasString,0);
        opal_output_verbose(1, mrplus_jobclient_output,
                            "%s sending alias string %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), alias);
        buf = OBJ_NEW(opal_buffer_t);
        if (OPAL_SUCCESS != (rc = opal_dss.pack(buf, &alias, 1, OPAL_STRING))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buf);
        }
        if (0 > (rc = orte_rml.send_buffer_nb(ORTE_PROC_MY_HNP, buf,
                                              ORTE_RML_TAG_MAX+2, 0,
                                              orte_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(rc);
            OBJ_RELEASE(buf);
        }
    }
}

static void hostname_aliases(int status, orte_process_name_t* sender,
                             opal_buffer_t* buffer, orte_rml_tag_t tag,
                             void* cbdata)
{
    char *aliasString, **aliases;
    orte_node_t *node;
    orte_proc_t *proc;
    orte_job_t *jdata;
    int rc, n, j;
    bool preexists;

    /* unpack the list of aliases */
    n=1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &aliasString, &n, OPAL_STRING))) {
        ORTE_ERROR_LOG(rc);
        return;
    }

    opal_output_verbose(1, mrplus_jobclient_output,
                        "%s received alias string %s from %s",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                        aliasString, ORTE_NAME_PRINT(sender));

    /* find the host for this process */
    if (NULL == (jdata = orte_get_job_data_object(sender->jobid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        return;
    }
    if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, sender->vpid))) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        return;
    }
    if (NULL == (node = proc->node)) {
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
        ORTE_TERMINATE(ORTE_ERROR_DEFAULT_EXIT_CODE);
        return;
    }

    /* split the string to get the individual aliases */
    aliases = opal_argv_split(aliasString, ',');

    /* add each alias to those for the node */
    for (n=0; NULL != aliases[n]; n++) {
        /* if the nodename matches this alias, ignore it */
        if (0 == strcmp(node->name, aliases[n])) {
            continue;
        }
        /* if we already have this alias, ignore it */
        preexists = false;
        for (j=0; NULL != node->alias[j]; j++) {
            if (0 == strcmp(node->alias[j], aliases[n])) {
                preexists = true;
                break;
            }
        }
        if (!preexists) {
            opal_argv_append_nosize(&node->alias, aliases[n]);
        }
    }

    /* cleanup */
    opal_argv_free(aliases);
}
