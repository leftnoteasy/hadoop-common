package org.apache.hadoop.yarn.api;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceRequestPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceRequestProto;
import org.junit.Test;

public class TestResourceRequest {
  @Test
  public void testSerdeWithExistingContainerId() {
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    ResourceRequest rr = ResourceRequest.newInstance(Priority.UNDEFINED, "",
        Resource.newInstance(1023, 3), 3, true, containerId);

    ResourceRequestProto p = ((ResourceRequestPBImpl) rr).getProto();
    ResourceRequest rr1 = new ResourceRequestPBImpl(p);

    // check values
    Assert.assertTrue(rr1.getExistingContainerId().equals(containerId));
  }

  @Test
  public void testSerdeWithoutExistingContainerId() {
    ResourceRequest rr = ResourceRequest.newInstance(Priority.UNDEFINED, "",
        Resource.newInstance(1023, 3), 3, true);

    ResourceRequestProto p = ((ResourceRequestPBImpl) rr).getProto();
    ResourceRequest rr1 = new ResourceRequestPBImpl(p);

    // check values
    Assert.assertNull(rr1.getExistingContainerId());
  }

  @Test
  public void testEquals() {
    /* case 1, same container id, capability, different with other fields */
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    ResourceRequest rr = ResourceRequest.newInstance(Priority.UNDEFINED, "",
        Resource.newInstance(1023, 3), 3, true, containerId);

    containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    ResourceRequest rr1 = ResourceRequest.newInstance(Priority.UNDEFINED,
        "xxx", Resource.newInstance(1023, 3), 3, false, containerId);

    Assert.assertTrue(rr.equals(rr1));
    
    /* case 2, same container id, different capability */
    containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    rr = ResourceRequest.newInstance(Priority.UNDEFINED, "",
        Resource.newInstance(1023, 4), 3, true, containerId);

    containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    rr1 = ResourceRequest.newInstance(Priority.UNDEFINED,
        "xxx", Resource.newInstance(1023, 3), 3, false, containerId);

    Assert.assertFalse(rr.equals(rr1));
    
    /* case 3, differnt container id, same capability */
    containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 6);
    rr = ResourceRequest.newInstance(Priority.UNDEFINED, "",
        Resource.newInstance(1023, 4), 3, true, containerId);

    containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), 7);
    rr1 = ResourceRequest.newInstance(Priority.UNDEFINED,
        "xxx", Resource.newInstance(1023, 4), 3, false, containerId);

    Assert.assertFalse(rr.equals(rr1));
    
    /* case 4, no container id, same fields */
    rr = ResourceRequest.newInstance(Priority.UNDEFINED, "xxx",
        Resource.newInstance(1023, 4), 3, true);

    rr1 = ResourceRequest.newInstance(Priority.UNDEFINED,
        "xxx", Resource.newInstance(1023, 4), 3, true);

    Assert.assertTrue(rr.equals(rr1));
    
    /* case 2, no container id, different fields */
    rr = ResourceRequest.newInstance(Priority.UNDEFINED, "",
        Resource.newInstance(1023, 4), 3, true);

    rr1 = ResourceRequest.newInstance(Priority.UNDEFINED,
        "xxx", Resource.newInstance(1023, 3), 3, true);

    Assert.assertFalse(rr.equals(rr1));
  }
}
