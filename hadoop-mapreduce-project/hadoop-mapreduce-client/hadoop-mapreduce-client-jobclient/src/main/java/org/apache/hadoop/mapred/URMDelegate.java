package org.apache.hadoop.mapred;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ompi.OmpiJobClient;
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
  
  public URMDelegate(Configuration conf) {
    
  }
  
  @Override
  public GetNewApplicationResponse getNewApplication(
      GetNewApplicationRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public KillApplicationResponse forceKillApplication(
      KillApplicationRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetApplicationReportResponse getApplicationReport(
      GetApplicationReportRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetClusterMetricsResponse getClusterMetrics(
      GetClusterMetricsRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetAllApplicationsResponse getAllApplications(
      GetAllApplicationsRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetClusterNodesResponse getClusterNodes(GetClusterNodesRequest request)
      throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetQueueInfoResponse getQueueInfo(GetQueueInfoRequest request)
      throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetQueueUserAclsInfoResponse getQueueUserAcls(
      GetQueueUserAclsInfoRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetDelegationTokenResponse getDelegationToken(
      GetDelegationTokenRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }
  
  String getFileSystemName() {
    return null;
  }

  public ApplicationReport getApplicationReport(ApplicationId applicationId) {
    // TODO Auto-generated method stub
    return null;
  }

  public ApplicationId submitApplication(ApplicationSubmissionContext appContext) {
    OmpiJobClient jc = new OmpiJobClient();
    
    // add files
    String files = "";
    Map<String, LocalResource> localResources = appContext.getAMContainerSpec().getLocalResources();
    for (Entry<String, LocalResource> entry : localResources.entrySet()) {
      LocalResource res = entry.getValue();
      if (files != "") {
        files = files + "," + res.getResource();
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
    
    // add and submit it!
    jc.addAppMaster(commands, null, 0L);
    jc.runAM();
    
    // create ApplicationId
    ApplicationId ret = Records.newRecord(ApplicationId.class);
    ret.setClusterTimestamp(0L);
    ret.setId(jc.getJobId());
    
    return ret;
  }

  public void killApplication(ApplicationId appId) {
    // TODO Auto-generated method stub
    
  }

  public ApplicationId getApplicationId() {
    // TODO Auto-generated method stub
    return null;
  }

}
