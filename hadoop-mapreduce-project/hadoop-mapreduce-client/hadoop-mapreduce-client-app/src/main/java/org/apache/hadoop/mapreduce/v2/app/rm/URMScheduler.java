package org.apache.hadoop.mapreduce.v2.app.rm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.urm.URMUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.FinishApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.util.Records;

public class URMScheduler implements AMRMProtocol {
  Configuration conf;
  int appId;
  
  public URMScheduler(Configuration conf, int appId) {
    this.conf = conf;
    this.appId = appId;
  }

  /**
   * write the app report to a local file
   */
  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnRemoteException {
    try {
      FileSystem fs = FileSystem.get(conf);
      Path appReportPath = URMUtils.getAppReportPath(conf, appId);
      FSDataOutputStream os = fs.create(appReportPath, true);
      URMUtils.saveApplicationReport(os,
          URMUtils.convertApplicationReport(request));
      os.close();
      
      /* fill the response */
      RegisterApplicationMasterResponse response = Records
          .newRecord(RegisterApplicationMasterResponse.class);
      Resource resource = Records.newRecord(Resource.class);
      resource.setMemory(10240);
      response.setMaximumResourceCapability(resource);
      response.setMinimumResourceCapability(resource);

      return response;
    } catch (IOException e) {
      throw new YarnException(e);
    }
  }

  @Override
  public FinishApplicationMasterResponse finishApplicationMaster(
      FinishApplicationMasterRequest request) throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request)
      throws YarnRemoteException {
    // TODO Auto-generated method stub
    return null;
  }

}
