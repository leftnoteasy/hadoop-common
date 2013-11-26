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
    List<ContainerId> succeedChanged = new ArrayList<ContainerId>();
    List<ContainerId> failChanged = new ArrayList<ContainerId>();

    for (int i = 0; i < 10; i++) {
      succeedChanged.add(getNextContainerId());
    }
    
    for (int i = 0; i < 8; i++) {
      failChanged.add(getNextContainerId());
    }

    ChangeContainersResourceResponse response = ChangeContainersResourceResponse
        .newInstance(succeedChanged, failChanged);

    // serde
    ChangeContainersResourceResponseProto proto = ((ChangeContainersResourceResponsePBImpl)response)
        .getProto();
    response = new ChangeContainersResourceResponsePBImpl(proto);

    // check value
    Assert.assertEquals(response.getSucceedChangedContainers().size(),
        succeedChanged.size());
    Assert.assertEquals(response.getFailedChangedContainers().size(),
        failChanged.size());
    for (int i = 0; i < succeedChanged.size(); i++) {
      Assert.assertTrue(response.getSucceedChangedContainers().get(i)
          .equals(succeedChanged.get(i)));
    }
    for (int i = 0; i < failChanged.size(); i++) {
      Assert.assertTrue(response.getFailedChangedContainers().get(i)
          .equals(failChanged.get(i)));
    }
  }
  
  @Test
  public void testChangeContainersResourceRequestWithNull() {
    ChangeContainersResourceResponse request = ChangeContainersResourceResponse.newInstance(null, null);
    
    // serde
    ChangeContainersResourceResponseProto proto = ((ChangeContainersResourceResponsePBImpl)request).getProto();
    request = new ChangeContainersResourceResponsePBImpl(proto);
    
    // check value
    Assert.assertEquals(0, request.getSucceedChangedContainers().size());
    Assert.assertEquals(0, request.getFailedChangedContainers().size());
  }
}
