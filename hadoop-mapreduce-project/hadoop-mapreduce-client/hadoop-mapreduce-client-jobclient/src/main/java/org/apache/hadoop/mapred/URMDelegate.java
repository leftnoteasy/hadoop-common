package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ompi.OmpiJobClient;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.urm.URMUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.ClientRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetAllApplicationsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationReportResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterNodesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetDelegationTokenResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetQueueUserAclsInfoResponse;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.KillApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

public class URMDelegate implements ClientRMProtocol {
  
  class AMRunnable extends Thread {
    OmpiJobClient jc;
    
    public AMRunnable(OmpiJobClient jc) {
      this.jc = jc;
    }
    
    public OmpiJobClient getJC() {
      return jc;
    }

    @Override
    public void run() {
      jc.runAM();
    }
  }
  
  Map<Integer,AMRunnable> amThreads = new HashMap<Integer, AMRunnable>();
  int lastId = -1;
  Configuration conf = null;
  
  public URMDelegate(Configuration conf) {
    this.conf = conf;
  }
  
  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnRemoteException {
    OmpiJobClient jc = new OmpiJobClient();
    AMRunnable amThread = new AMRunnable(jc);
    
    int rid = Math.abs(new Random(System.currentTimeMillis()).nextInt()) % 10000;
    amThreads.put(rid, amThread);
    lastId = rid;
    
    // set response
    GetNewApplicationResponse newAppResponse = Records.newRecord(GetNewApplicationResponse.class);
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setId(rid);
    newAppResponse.setApplicationId(appId);
    
    return newAppResponse;
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnRemoteException {
    ApplicationSubmissionContext context = request.getApplicationSubmissionContext();
    submitApplication(context);
    return Records.newRecord(SubmitApplicationResponse.class);
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnRemoteException {
    return null;
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnRemoteException {
    GetApplicationReportResponse response = Records.newRecord(GetApplicationReportResponse.class);
    try {
      response.setApplicationReport(getApplicationReport(request.getApplicationId()));
    } catch (IOException e) {
      throw new YarnException(e);
    } catch (InterruptedException e) {
      throw new YarnException(e);
    }
    return response;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnRemoteException {
    return null;
  }

  @Override
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {
    return null;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnRemoteException {
    return null;
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnRemoteException {
    return null;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
    return null;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnRemoteException {
    return null;
  }
  
  String getFileSystemName() throws IOException {
    return FileSystem.get(conf).getUri().toString();
  }

  public ApplicationReport getApplicationReport(ApplicationId applicationId) throws IOException, InterruptedException {
    FileSystem fs = FileSystem.get(conf);
    Path path = URMUtils.getAppReportPath(conf, applicationId.getId());
    boolean fileExists = false;
    
    // retry until file created or exceeds retry limit
    int nRetry = 0;
    while (nRetry < MRConfig.MAX_APPMASTER_REPORT_RETRY) {
      fileExists = fs.exists(path);
      if (fileExists) {
        break;
      }
      Thread.sleep(1000);
      nRetry++;
    }
    
    if (fileExists) {
      // read ApplicationReport out
      FSDataInputStream is = fs.open(path);
      ApplicationReport report = URMUtils.loadApplicationReport(is);
      is.close();
      return report;
    }
    
    return null;
  }

  public ApplicationId submitApplication(ApplicationSubmissionContext appContext) {
    int appId = appContext.getApplicationId().getId();
    AMRunnable amThread = amThreads.get(appId);
    
    if (null == amThread) {
      throw new RuntimeException("this app not submitted, appid:" + appId);
    }
    OmpiJobClient jc = amThread.getJC();
    
    // add files
    String files = "";
    Map<String, LocalResource> localResources = appContext.getAMContainerSpec().getLocalResources();
    for (Entry<String, LocalResource> entry : localResources.entrySet()) {
      LocalResource res = entry.getValue();
      if (files != "") {
        files = files + "," + res.getResource().getFile();
      } else {
        files = res.getResource().getFile();
      }
    }
    jc.addFiles(files);
    
    // add envs 
    Map<String, String> envars = appContext.getAMContainerSpec().getEnvironment();
    for (Entry<String, String> entry : envars.entrySet()) {
      String envAdd = entry.getKey() + "=" + entry.getValue();
      jc.addEnv(envAdd);
    }
    
    // add args
    String[] commands = appContext.getAMContainerSpec().getCommands().toArray(new String[0]);
    if (1 == commands.length) {
      String[] args = commands[0].split(" ");
      args[0] = "java";
      jc.addAppMaster(args, null, 0L);
    } else {
      throw new YarnException("commands specified in app context should only = 1");
    }
    /*
    String[] args = new String[1];
    args[0] = "pwd";
    jc.addAppMaster(args, null, 0L);
    */
    
    // start another thread run this
    amThread.start();
    
    return getApplicationId();
  }

  public void killApplication(ApplicationId appId) {
    Thread t = amThreads.get(appId.getId());
    if (t.isAlive()) {
      t.interrupt();
    }
    amThreads.remove(appId.getId());
  }

  public ApplicationId getApplicationId() {
    if (lastId >= 0) {
      ApplicationId id = Records.newRecord(ApplicationId.class);
      id.setId(lastId);
      return id;
    }
    
    return null;
  }
}
