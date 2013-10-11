package org.apache.hadoop.yarn.api;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceChangeContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceChangeContextProto;
import org.junit.Test;

public class TestResourceChangeContext {
  @Test
  public void testResourceChangeContext() {
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    Resource resource = Resource.newInstance(1023, 3);
    ResourceChangeContext context = ResourceChangeContext.newInstance(
        containerId, resource);
    
    // to proto and get it back
    ResourceChangeContextProto proto = ((ResourceChangeContextPBImpl)context).getProto();
    ResourceChangeContext contextRecover = new ResourceChangeContextPBImpl(proto);
    
    // check value
    Assert.assertTrue(contextRecover.getExistingContainerId().equals(containerId));
    Assert.assertTrue(contextRecover.getTargetCapability().equals(resource));
  }
  
  @Test
  public void testResourceChangeContextWithNullField() {
    ResourceChangeContext context = ResourceChangeContext.newInstance(null, null);
    
    // to proto and get it back
    ResourceChangeContextProto proto = ((ResourceChangeContextPBImpl)context).getProto();
    ResourceChangeContext contextRecover = new ResourceChangeContextPBImpl(proto);
    
    // check value
    Assert.assertNull(contextRecover.getExistingContainerId());
    Assert.assertNull(contextRecover.getTargetCapability());
  }
}
