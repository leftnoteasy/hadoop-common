package org.apache.hadoop.mapreduce.urm;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.util.Records;

public class URMUtils {
  /**
   * this is a ugly workaround for POC, AM will write its port and address to a
   * file (assume on a global file system) And URMDelegate will retrive this
   * file and read content out
   * 
   * @param conf
   * @return AppReport path
   * @throws IOException 
   */
  public static Path getAppReportPath(Configuration conf, int id) throws IOException {
    String appMasterRoot = conf.get(MRConfig.APPMASTER_REPORT_PATH, MRConfig.DEFAULT_APPMASTER_REPORT_PATH);
    Path reportPath = new Path(appMasterRoot, "app_master_report_" + id);
    return reportPath;
  }
  
  public static ApplicationReport convertApplicationReport(RegisterApplicationMasterRequest request) {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setYarnApplicationState(YarnApplicationState.RUNNING);
    report.setApplicationId(request.getApplicationAttemptId().getApplicationId());
    report.setCurrentApplicationAttemptId(request.getApplicationAttemptId());
    report.setHost(request.getHost());
    report.setTrackingUrl(request.getTrackingUrl());
    report.setRpcPort(request.getRpcPort());
    return report;
  }
  
  public static void saveApplicationReport(DataOutputStream os, ApplicationReport report) throws IOException {
    // app id
    os.writeInt(report.getApplicationId().getId());

    // port
    os.writeInt(report.getRpcPort());
    
    // host
    os.writeInt(report.getHost().length());
    os.write(report.getHost().getBytes());
    
    // tracking URL
    os.writeInt(report.getTrackingUrl().length());
    os.write(report.getTrackingUrl().getBytes());
    
    // state
    os.writeInt(report.getYarnApplicationState().ordinal());
  }
  
  public static ApplicationReport loadApplicationReport(DataInputStream is) throws IOException {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    int id = is.readInt();
    int port = is.readInt();
    int hostLen = is.readInt();
    byte[] hostBuffer = new byte[hostLen];
    is.read(hostBuffer);
    
    // create ApplicationId and set
    ApplicationId appId = Records.newRecord(ApplicationId.class);
    appId.setId(id);
    report.setApplicationId(appId);
    
    // set port
    report.setRpcPort(port);
    
    // create host and set
    String host = new String(hostBuffer);
    report.setHost(host);
    
    // create tracking URL and set
    int trackingUrlLen = is.readInt();
    byte[] trackingUrlBuffer = new byte[trackingUrlLen];
    is.read(trackingUrlBuffer);
    String trackingUrl = new String(trackingUrlBuffer);
    report.setTrackingUrl(trackingUrl);
    
    // state
    int ordinal = is.readInt();
    report.setYarnApplicationState(YarnApplicationState.values()[ordinal]);
    
    report.setUser(UserGroupInformation.getCurrentUser().getUserName());
    
    return report;
  }
}
