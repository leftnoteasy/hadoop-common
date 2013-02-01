package org.apache.hadoop.mapreduce.urm;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

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
  
  public static void saveApplicationReport(DataOutputStream os, ApplicationReport report) {
    
  }
  
  public static ApplicationReport loadApplicationReport(DataInputStream is) {
    return null;
  }
}
