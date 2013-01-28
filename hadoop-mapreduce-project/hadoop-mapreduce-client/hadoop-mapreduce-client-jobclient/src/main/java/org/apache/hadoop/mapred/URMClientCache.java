package org.apache.hadoop.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;

public class URMClientCache {
  URMDelegate urmDelegate;
  Configuration conf;
  
  public URMClientCache(Configuration conf, URMDelegate urmDelegate) {
    this.urmDelegate = urmDelegate;
    this.conf = conf;
  }
  
  ClientServiceDelegate getClient(JobID jobId) {
    return null;
  }

  public MRClientProtocol getInitializedHSProxy() {
    // TODO Auto-generated method stub
    return null;
  }
}
