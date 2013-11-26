package org.apache.hadoop.yarn.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ChangeContainersResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceRequestProto;
import org.junit.Test;

public class TestChangeContainersResourceRequest {
  int id = 0;

  private ResourceChangeContext getNextResourceChangeContext() {
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), id++);
    Resource resource = Resource.newInstance(1023, 3);
    ResourceChangeContext rcContext = ResourceChangeContext.newInstance(
        containerId, resource);
    return rcContext;
  }

  @Test
  public void testChangeContainersResourceRequest() {
    List<ResourceIncreaseContext> containerToIncrease = new ArrayList<ResourceIncreaseContext>();
    List<ResourceChangeContext> containerToDecrease = new ArrayList<ResourceChangeContext>();

    for (int i = 0; i < 10; i++) {
      ResourceIncreaseContext ctx = ResourceIncreaseContext.newInstance(
          getNextResourceChangeContext(), null);
      containerToIncrease.add(ctx);
    }
    for (int i = 0; i < 5; i++) {
      ResourceChangeContext ctx = getNextResourceChangeContext();
      containerToDecrease.add(ctx);
    }

    ChangeContainersResourceRequest request = ChangeContainersResourceRequest
        .newInstance(containerToIncrease, containerToDecrease);

    // serde
    ChangeContainersResourceRequestProto proto = ((ChangeContainersResourceRequestPBImpl) request)
        .getProto();
    request = new ChangeContainersResourceRequestPBImpl(proto);

    // check value
    Assert.assertEquals(request.getContainersToIncrease().size(),
        containerToIncrease.size());
    Assert.assertEquals(request.getContainersToDecrease().size(),
        containerToDecrease.size());
    for (int i = 0; i < containerToIncrease.size(); i++) {
      Assert.assertTrue(request.getContainersToIncrease().get(i)
          .equals(containerToIncrease.get(i)));
    }
    for (int i = 0; i < containerToDecrease.size(); i++) {
      Assert.assertTrue(request.getContainersToDecrease().get(i)
          .equals(containerToDecrease.get(i)));
    }
  }
  
  @Test
  public void testChangeContainersResourceRequestWithNull() {
    ChangeContainersResourceRequest request = ChangeContainersResourceRequest.newInstance(null, null);
    
    // serde
    ChangeContainersResourceRequestProto proto = ((ChangeContainersResourceRequestPBImpl)request).getProto();
    request = new ChangeContainersResourceRequestPBImpl(proto);
    
    // check value
    Assert.assertEquals(0, request.getContainersToIncrease().size());
    Assert.assertEquals(0, request.getContainersToDecrease().size());
  }
}
