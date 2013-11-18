package org.apache.hadoop.yarn.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateRequestPBImpl;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateRequestProto;
import org.junit.Test;

public class TestAllocateRequest {
  @Test
  public void testAllcoateRequestWithIncrease() {
    List<ResourceChangeContext> incRequests = new ArrayList<ResourceChangeContext>();
    for (int i = 0; i < 3; i++) {
      incRequests.add(ResourceChangeContext.newInstance(null, Resource.newInstance(0, i)));
    }
    AllocateRequest r = AllocateRequest.newInstance(123, 0f, null, null, null, incRequests);
    
    // serde
    AllocateRequestProto p = ((AllocateRequestPBImpl)r).getProto();
    r = new AllocateRequestPBImpl(p);
    
    // check value
    Assert.assertEquals(123, r.getResponseId());
    Assert.assertEquals(incRequests.size(), r.getIncreaseRequests().size());
    
    for (int i = 0; i < incRequests.size(); i++) {
      Assert.assertEquals(r.getIncreaseRequests().get(i).getTargetCapability()
          .getVirtualCores(), incRequests.get(i).getTargetCapability()
          .getVirtualCores());   
    }
  }
  
  @Test
  public void testAllcoateRequestWithoutIncrease() {
    AllocateRequest r = AllocateRequest.newInstance(123, 0f, null, null, null, null);
    
    // serde
    AllocateRequestProto p = ((AllocateRequestPBImpl)r).getProto();
    r = new AllocateRequestPBImpl(p);
    
    // check value
    Assert.assertEquals(123, r.getResponseId());
    Assert.assertEquals(0, r.getIncreaseRequests().size());
  }
}
