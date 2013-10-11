package org.apache.hadoop.yarn.api;

import java.util.Arrays;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceIncreaseContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceIncreaseContextProto;
import org.junit.Test;

public class TestResourceIncreaseContext {
  @Test
  public void testResourceIncreaseContext() {
    byte[] identifier = new byte[] { 1, 2, 3, 4 };
    Token token = Token.newInstance(identifier, "", "".getBytes(), "");
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    Resource resource = Resource.newInstance(1023, 3);
    ResourceChangeContext rcContext = ResourceChangeContext.newInstance(
        containerId, resource);
    ResourceIncreaseContext ctx = ResourceIncreaseContext.newInstance(
        rcContext, token);

    // get proto and recover to ctx
    ResourceIncreaseContextProto proto = ((ResourceIncreaseContextPBImpl) ctx)
        .getProto();
    ctx = new ResourceIncreaseContextPBImpl(proto);

    // check values
    Assert.assertTrue(Arrays.equals(ctx.getContainerToken().getIdentifier()
        .array(), identifier));
    Assert.assertTrue(ctx.getResourceChangeContext().equals(rcContext));
  }
  
  @Test
  public void testResourceIncreaseContextWithNull() {
    ResourceIncreaseContext ctx = ResourceIncreaseContext.newInstance(null, null);
    
    // get proto and recover to ctx;
    ResourceIncreaseContextProto proto = ((ResourceIncreaseContextPBImpl) ctx)
        .getProto();
    ctx = new ResourceIncreaseContextPBImpl(proto);

    // check values
    Assert.assertNull(ctx.getContainerToken());
    Assert.assertNull(ctx.getResourceChangeContext());
  }
}
