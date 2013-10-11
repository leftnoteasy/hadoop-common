package org.apache.hadoop.yarn.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ChangeContainersResourceResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceResponseProto;
import org.junit.Test;

public class TestChangeContainersResourceResponse {
  int id = 0;

  private ContainerId getNextContainerId() {
    ContainerId containerId = ContainerId
        .newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), id++);
    return containerId;
  }

  @Test
  public void testChangeContainersResourceRequest() {
    List<ContainerId> succInc = new ArrayList<ContainerId>();
    List<ContainerId> succDec = new ArrayList<ContainerId>();
    List<ContainerId> failInc = new ArrayList<ContainerId>();
    List<ContainerId> failDec = new ArrayList<ContainerId>();

    for (int i = 0; i < 10; i++) {
      succInc.add(getNextContainerId());
    }
    
    for (int i = 0; i < 9; i++) {
      succDec.add(getNextContainerId());
    }
    
    for (int i = 0; i < 8; i++) {
      failInc.add(getNextContainerId());
    }
    
    for (int i = 0; i < 7; i++) {
      failDec.add(getNextContainerId());
    }

    ChangeContainersResourceResponse response = ChangeContainersResourceResponse
        .newInstance(succInc, succDec, failInc, failDec);

    // serde
    ChangeContainersResourceResponseProto proto = ((ChangeContainersResourceResponsePBImpl)response)
        .getProto();
    response = new ChangeContainersResourceResponsePBImpl(proto);

    // check value
    Assert.assertEquals(response.getSucceedIncreasedContainers().size(),
        succInc.size());
    Assert.assertEquals(response.getSucceedDecreasedContainers().size(),
        succDec.size());
    Assert.assertEquals(response.getFailedIncreasedContainers().size(),
        failInc.size());
    Assert.assertEquals(response.getFailedDecreasedContainers().size(),
        failDec.size());
    for (int i = 0; i < succInc.size(); i++) {
      Assert.assertTrue(response.getSucceedIncreasedContainers().get(i)
          .equals(succInc.get(i)));
    }
    for (int i = 0; i < succDec.size(); i++) {
      Assert.assertTrue(response.getSucceedDecreasedContainers().get(i)
          .equals(succDec.get(i)));
    }
    for (int i = 0; i < failInc.size(); i++) {
      Assert.assertTrue(response.getFailedIncreasedContainers().get(i)
          .equals(failInc.get(i)));
    }
    for (int i = 0; i < failDec.size(); i++) {
      Assert.assertTrue(response.getFailedDecreasedContainers().get(i)
          .equals(failDec.get(i)));
    }
  }
  
  @Test
  public void testChangeContainersResourceRequestWithNull() {
    ChangeContainersResourceResponse request = ChangeContainersResourceResponse.newInstance(null, null, null, null);
    
    // serde
    ChangeContainersResourceResponseProto proto = ((ChangeContainersResourceResponsePBImpl)request).getProto();
    request = new ChangeContainersResourceResponsePBImpl(proto);
    
    // check value
    Assert.assertEquals(0, request.getSucceedIncreasedContainers().size());
    Assert.assertEquals(0, request.getSucceedDecreasedContainers().size());
    Assert.assertEquals(0, request.getFailedIncreasedContainers().size());
    Assert.assertEquals(0, request.getFailedDecreasedContainers().size());
  }
}
