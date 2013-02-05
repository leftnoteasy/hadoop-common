package org.apache.hadoop.mapred;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.ompi.OmpiCommon;
import org.apache.hadoop.mapreduce.Cluster.JobTrackerStatus;
import org.apache.hadoop.mapreduce.ClusterMetrics;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.QueueAclsInfo;
import org.apache.hadoop.mapreduce.QueueInfo;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskTrackerInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.protocol.ClientProtocol;
import org.apache.hadoop.mapreduce.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.LogParams;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

public class URMRunner implements ClientProtocol {

  private static final Log LOG = LogFactory.getLog(YARNRunner.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  private URMDelegate urmDelegate;
  private URMClientCache clientCache;
  private Configuration conf;
  private final FileContext defaultFileContext;
  private OmpiCommon common = null;

  /*
   * usually is false unless the jobclient getdelegation token is called. This
   * is a hack wherein we do return a token from RM on getDelegationtoken but
   * due to the restricted api on jobclient we just add a job history DT token
   * when submitting a job.
   */
  private static final boolean DEFAULT_HS_DELEGATION_TOKEN_REQUIRED = false;

  /**
   * Yarn runner incapsulates the client interface of yarn
   * 
   * @param conf
   *          the configuration object for the client
   */
  public URMRunner(Configuration conf) {
    this(conf, new URMDelegate(new YarnConfiguration(conf)));
  }

  /**
   * Similar to {@link #YARNRunner(Configuration)} but allowing injecting
   * {@link ResourceMgrDelegate}. Enables mocking and testing.
   * 
   * @param conf
   *          the configuration object for the client
   * @param resMgrDelegate
   *          the resourcemanager client handle.
   */
  public URMRunner(Configuration conf, URMDelegate resMgrDelegate) {
    this(conf, resMgrDelegate, new URMClientCache(conf, resMgrDelegate));
  }

  /**
   * Similar to
   * {@link YARNRunner#YARNRunner(Configuration, ResourceMgrDelegate)} but
   * allowing injecting {@link ClientCache}. Enable mocking and testing.
   * 
   * @param conf
   *          the configuration object
   * @param resMgrDelegate
   *          the resource manager delegate
   * @param clientCache
   *          the client cache object.
   */
  public URMRunner(Configuration conf, URMDelegate urmDelegate,
      URMClientCache clientCache) {
    this.conf = conf;
    try {
      this.urmDelegate = urmDelegate;
      this.clientCache = clientCache;
      this.defaultFileContext = FileContext.getFileContext(this.conf);
    } catch (UnsupportedFileSystemException ufe) {
      throw new RuntimeException("Error in instantiating YarnClient", ufe);
    }
    initOmpi();
  }
  
  private void initOmpi() {
    common = new OmpiCommon(0);
  }

  @Private
  /**
   * Used for testing mostly.
   * @param resMgrDelegate the resource manager delegate to set to.
   */
  public void setResourceMgrDelegate(URMDelegate urmDelegate) {
    this.urmDelegate = urmDelegate;
  }

  @Override
  public void cancelDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    // TODO, need implement this
  }

  @Override
  public TaskTrackerInfo[] getActiveTrackers() throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public JobStatus[] getAllJobs() throws IOException, InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public TaskTrackerInfo[] getBlacklistedTrackers() throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public ClusterMetrics getClusterMetrics() throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  private Token<?> getDelegationTokenFromHS(MRClientProtocol hsProxy,
      Text renewer) throws IOException, InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public Token<DelegationTokenIdentifier> getDelegationToken(Text renewer)
      throws IOException, InterruptedException {
    // The token is only used for serialization. So the type information
    // mismatch should be fine.
    // TODO, need implement this
    return null;
  }

  @Override
  public String getFilesystemName() throws IOException, InterruptedException {
    return urmDelegate.getFileSystemName();
  }

  @Override
  public JobID getNewJobID() throws IOException, InterruptedException {
    GetNewApplicationRequest request = Records.newRecord(GetNewApplicationRequest.class);
    GetNewApplicationResponse response = urmDelegate.getNewApplication(request);
    JobID jobID = new JobID("orte_job", response.getApplicationId().getId());
    return jobID;
  }

  @Override
  public QueueInfo getQueue(String queueName) throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public QueueAclsInfo[] getQueueAclsForCurrentUser() throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public QueueInfo[] getQueues() throws IOException, InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public QueueInfo[] getRootQueues() throws IOException, InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public QueueInfo[] getChildQueues(String parent) throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public String getStagingAreaDir() throws IOException, InterruptedException {
    return "/tmp/staging-area";
  }

  @Override
  public String getSystemDir() throws IOException, InterruptedException {
    Path sysDir = new Path(MRJobConfig.JOB_SUBMIT_DIR);
    return sysDir.toString();
  }

  @Override
  public long getTaskTrackerExpiryInterval() throws IOException,
      InterruptedException {
    // TODO, need implement this
    return -1;
  }

  @Override
  public JobStatus submitJob(JobID jobId, String jobSubmitDir, Credentials ts)
      throws IOException, InterruptedException {

    /* check if we have a hsproxy, if not, no need */
    MRClientProtocol hsProxy = clientCache.getInitializedHSProxy();
    if (hsProxy != null) {
      // JobClient will set this flag if getDelegationToken is called, if so,
      // get
      // the delegation tokens for the HistoryServer also.
      if (conf.getBoolean(JobClient.HS_DELEGATION_TOKEN_REQUIRED,
          DEFAULT_HS_DELEGATION_TOKEN_REQUIRED)) {
        Token hsDT = getDelegationTokenFromHS(hsProxy,
            new Text(conf.get(JobClient.HS_DELEGATION_TOKEN_RENEWER)));
        ts.addToken(hsDT.getService(), hsDT);
      }
    }

    // Upload only in security mode: TODO
    Path applicationTokensFile = new Path(jobSubmitDir,
        MRJobConfig.APPLICATION_TOKENS_FILE);
    try {
      ts.writeTokenStorageFile(applicationTokensFile, conf);
    } catch (IOException e) {
      throw new YarnException(e);
    }

    // Construct necessary information to start the MR AM
    ApplicationSubmissionContext appContext = createApplicationSubmissionContext(
        conf, jobSubmitDir, ts);

    // Submit to ResourceManager
    ApplicationId applicationId = urmDelegate.submitApplication(appContext);

    ApplicationReport appMaster = urmDelegate
        .getApplicationReport(applicationId);
    String diagnostics = (appMaster == null ? "application report is null"
        : appMaster.getDiagnostics());
    if (appMaster == null
        || appMaster.getYarnApplicationState() == YarnApplicationState.FAILED
        || appMaster.getYarnApplicationState() == YarnApplicationState.KILLED) {
      throw new IOException("Failed to run job : " + diagnostics);
    }
    return clientCache.getClient(jobId).getJobStatus(jobId);
  }

  private LocalResource createApplicationResource(FileContext fs, Path p,
      LocalResourceType type) throws IOException {
    LocalResource rsrc = recordFactory.newRecordInstance(LocalResource.class);
    FileStatus rsrcStat = fs.getFileStatus(p);
    rsrc.setResource(ConverterUtils.getYarnUrlFromPath(fs
        .getDefaultFileSystem().resolvePath(rsrcStat.getPath())));
    rsrc.setSize(rsrcStat.getLen());
    rsrc.setTimestamp(rsrcStat.getModificationTime());
    rsrc.setType(type);
    rsrc.setVisibility(LocalResourceVisibility.APPLICATION);
    return rsrc;
  }

  public ApplicationSubmissionContext createApplicationSubmissionContext(
      Configuration jobConf, String jobSubmitDir, Credentials ts)
      throws IOException {
    ApplicationId applicationId = urmDelegate.getApplicationId();

    // Setup resource requirements
    Resource capability = recordFactory.newRecordInstance(Resource.class);
    capability.setMemory(conf.getInt(MRJobConfig.MR_AM_VMEM_MB,
        MRJobConfig.DEFAULT_MR_AM_VMEM_MB));
    LOG.debug("AppMaster capability = " + capability);

    // Setup LocalResources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    Path jobConfPath = new Path(jobSubmitDir, MRJobConfig.JOB_CONF_FILE);

    URL yarnUrlForJobSubmitDir = ConverterUtils
        .getYarnUrlFromPath(defaultFileContext.getDefaultFileSystem()
            .resolvePath(
                defaultFileContext.makeQualified(new Path(jobSubmitDir))));
    LOG.debug("Creating setup context, jobSubmitDir url is "
        + yarnUrlForJobSubmitDir);

    localResources.put(
        MRJobConfig.JOB_CONF_FILE,
        createApplicationResource(defaultFileContext, jobConfPath,
            LocalResourceType.FILE));
    if (jobConf.get(MRJobConfig.JAR) != null) {
      Path jobJarPath = new Path(jobConf.get(MRJobConfig.JAR));
      LocalResource rc = createApplicationResource(defaultFileContext,
          jobJarPath, LocalResourceType.PATTERN);
      String pattern = conf.getPattern(JobContext.JAR_UNPACK_PATTERN,
          JobConf.UNPACK_JAR_PATTERN_DEFAULT).pattern();
      rc.setPattern(pattern);
      localResources.put(MRJobConfig.JOB_JAR, rc);
    } else {
      // Job jar may be null. For e.g, for pipes, the job jar is the hadoop
      // mapreduce jar itself which is already on the classpath.
      LOG.info("Job jar is not present. "
          + "Not adding any jar to the list of resources.");
    }

    // TODO gross hack
    for (String s : new String[] { MRJobConfig.JOB_SPLIT,
        MRJobConfig.JOB_SPLIT_METAINFO, MRJobConfig.APPLICATION_TOKENS_FILE }) {
      localResources.put(
          MRJobConfig.JOB_SUBMIT_DIR + "/" + s,
          createApplicationResource(defaultFileContext, new Path(jobSubmitDir,
              s), LocalResourceType.FILE));
    }

    // Setup security tokens
    ByteBuffer securityTokens = null;
    if (UserGroupInformation.isSecurityEnabled()) {
      DataOutputBuffer dob = new DataOutputBuffer();
      ts.writeTokenStorageToStream(dob);
      securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    }

    // Setup the command to run the AM
    List<String> vargs = new ArrayList<String>(8);
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");

    // TODO: why do we use 'conf' some places and 'jobConf' others?
    long logSize = TaskLog.getTaskLogLength(new JobConf(conf));
    String logLevel = jobConf.get(MRJobConfig.MR_AM_LOG_LEVEL,
        MRJobConfig.DEFAULT_MR_AM_LOG_LEVEL);
    
    // MRApps.addLog4jSystemProperties(logLevel, logSize, vargs);

    vargs.add(conf.get(MRJobConfig.MR_AM_COMMAND_OPTS,
        MRJobConfig.DEFAULT_MR_AM_COMMAND_OPTS));

    vargs.add(MRJobConfig.APPLICATION_MASTER_CLASS);
    
    Vector<String> vargsFinal = new Vector<String>(8);
    // Final commmand
    StringBuilder mergedCommand = new StringBuilder();
    for (CharSequence str : vargs) {
      mergedCommand.append(str).append(" ");
    }
    vargsFinal.add(mergedCommand.toString());

    LOG.debug("Command to launch container for ApplicationMaster is : "
        + mergedCommand);

    // Setup the CLASSPATH in environment
    // i.e. add { Hadoop jars, job jar, CWD } to classpath.
    Map<String, String> environment = new HashMap<String, String>();
    MRApps.setClasspath(environment, conf);
    setupContainerEnv(environment);

    // Parse distributed cache
    MRApps.setupDistributedCache(jobConf, localResources);

    Map<ApplicationAccessType, String> acls = new HashMap<ApplicationAccessType, String>(
        2);
    acls.put(ApplicationAccessType.VIEW_APP, jobConf.get(
        MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));
    acls.put(ApplicationAccessType.MODIFY_APP, jobConf.get(
        MRJobConfig.JOB_ACL_MODIFY_JOB, MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

    // Setup ContainerLaunchContext for AM container
    ContainerLaunchContext amContainer = BuilderUtils
        .newContainerLaunchContext(null, UserGroupInformation.getCurrentUser()
            .getShortUserName(), capability, localResources, environment,
            vargsFinal, null, securityTokens, acls);

    // Set up the ApplicationSubmissionContext
    ApplicationSubmissionContext appContext = recordFactory
        .newRecordInstance(ApplicationSubmissionContext.class);
    appContext.setApplicationId(applicationId); // ApplicationId
    appContext.setUser( // User name
        UserGroupInformation.getCurrentUser().getShortUserName());
    appContext.setQueue( // Queue name
        jobConf
            .get(JobContext.QUEUE_NAME, YarnConfiguration.DEFAULT_QUEUE_NAME));
    appContext.setApplicationName( // Job name
        jobConf.get(JobContext.JOB_NAME,
            YarnConfiguration.DEFAULT_APPLICATION_NAME));
    appContext.setCancelTokensWhenComplete(conf.getBoolean(
        MRJobConfig.JOB_CANCEL_DELEGATION_TOKEN, true));
    appContext.setAMContainerSpec(amContainer); // AM Container

    return appContext;
  }
  
  // setup envs that MRAppMaster needs
  private void setupContainerEnv(Map<String, String> env) {
    // create a containerId
    ApplicationAttemptId aaid = Records.newRecord(ApplicationAttemptId.class);
    aaid.setApplicationId(urmDelegate.getApplicationId());
    ContainerId cid = Records.newRecord(ContainerId.class);
    cid.setApplicationAttemptId(aaid);
    
    env.put(ApplicationConstants.AM_CONTAINER_ID_ENV, cid.toString());
    env.put(ApplicationConstants.NM_HOST_ENV, "localhost");
    env.put(ApplicationConstants.NM_HTTP_PORT_ENV, String.valueOf(8089));
    env.put(ApplicationConstants.NM_PORT_ENV, String.valueOf(5431));
    env.put(ApplicationConstants.APP_SUBMIT_TIME_ENV, String.valueOf(0L));
  }

  @Override
  public void setJobPriority(JobID arg0, String arg1) throws IOException,
      InterruptedException {
    // TODO, need implement this
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    // TODO, need implement this
    return -1;
  }

  @Override
  public long renewDelegationToken(Token<DelegationTokenIdentifier> arg0)
      throws IOException, InterruptedException {
    // TODO, need implement this
    return -1;
  }

  @Override
  public Counters getJobCounters(JobID arg0) throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public String getJobHistoryDir() throws IOException, InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public JobStatus getJobStatus(JobID jobID) throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public TaskCompletionEvent[] getTaskCompletionEvents(JobID arg0, int arg1,
      int arg2) throws IOException, InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public String[] getTaskDiagnostics(TaskAttemptID arg0) throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public TaskReport[] getTaskReports(JobID jobID, TaskType taskType)
      throws IOException, InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public void killJob(JobID arg0) throws IOException, InterruptedException {
    /* check if the status is not running, if not send kill to RM */
    JobStatus status = clientCache.getClient(arg0).getJobStatus(arg0);
    if (status.getState() != JobStatus.State.RUNNING) {
      urmDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
      return;
    }

    try {
      /* send a kill to the AM */
      clientCache.getClient(arg0).killJob(arg0);
      long currentTimeMillis = System.currentTimeMillis();
      long timeKillIssued = currentTimeMillis;
      while ((currentTimeMillis < timeKillIssued + 10000L)
          && (status.getState() != JobStatus.State.KILLED)) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException ie) {
          /** interrupted, just break */
          break;
        }
        currentTimeMillis = System.currentTimeMillis();
        status = clientCache.getClient(arg0).getJobStatus(arg0);
      }
    } catch (IOException io) {
      LOG.debug("Error when checking for application status", io);
    }
    if (status.getState() != JobStatus.State.KILLED) {
      urmDelegate.killApplication(TypeConverter.toYarn(arg0).getAppId());
    }
  }

  @Override
  public boolean killTask(TaskAttemptID arg0, boolean arg1) throws IOException,
      InterruptedException {
    return clientCache.getClient(arg0.getJobID()).killTask(arg0, arg1);
  }

  @Override
  public AccessControlList getQueueAdmins(String arg0) throws IOException {
    return null;
  }

  @Override
  public JobTrackerStatus getJobTrackerStatus() throws IOException,
      InterruptedException {
    // TODO, need implement this
    return null;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    // TODO, need implement this
    return null;
  }

  @Override
  public LogParams getLogFileParams(JobID jobID, TaskAttemptID taskAttemptID)
      throws IOException {
    // TODO, need implement this
    return null;
  }
}
