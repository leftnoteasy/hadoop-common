package org.apache.hadoop.mapred;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.v2.api.MRClientProtocol;

public class URMClientCache {
  URMDelegate urmDelegate;
  Configuration conf;
  Map<Integer, URMClientServiceDelegate> cache = new HashMap<Integer, URMClientServiceDelegate>();
  
  public URMClientCache(Configuration conf, URMDelegate urmDelegate) {
    this.urmDelegate = urmDelegate;
    this.conf = conf;
  }
  
  URMClientServiceDelegate getClient(JobID jobId) {
    if (cache.containsKey(jobId.getId())) {
      return cache.get(jobId.getId());
    } else {
      URMClientServiceDelegate delegate = new URMClientServiceDelegate(conf,
          urmDelegate, jobId, null);
      cache.put(jobId.getId(), delegate);
      return delegate;
    }
  }

  public MRClientProtocol getInitializedHSProxy() {
    return null;
  }
}
