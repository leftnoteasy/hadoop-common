/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.ChangeContainersResourceRequestPBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceRequestProto;
import org.junit.Test;

public class TestChangeContainersResourceRequest {
  int id = 0;

  private ContainerResourceDecrease getNextResourceChangeContext() {
    ContainerId containerId =
        ContainerId.newInstance(ApplicationAttemptId.newInstance(
            ApplicationId.newInstance(1234, 3), 3), id++);
    Resource resource = Resource.newInstance(1023, 3);
    ContainerResourceDecrease rcContext =
        ContainerResourceDecrease.newInstance(containerId, resource);
    return rcContext;
  }

  @Test
  public void testChangeContainersResourceRequest() {
    List<Token> containerToIncrease = new ArrayList<Token>();
    List<ContainerResourceDecrease> containerToDecrease =
        new ArrayList<ContainerResourceDecrease>();

    for (int i = 0; i < 10; i++) {
      Token ctx =
          Token.newInstance("identifier".getBytes(), "simple",
              "passwd".getBytes(), "service");
      containerToIncrease.add(ctx);
    }
    for (int i = 0; i < 5; i++) {
      ContainerResourceDecrease ctx = getNextResourceChangeContext();
      containerToDecrease.add(ctx);
    }

    ChangeContainersResourceRequest request =
        ChangeContainersResourceRequest.newInstance(containerToIncrease,
            containerToDecrease);

    // serde
    ChangeContainersResourceRequestProto proto =
        ((ChangeContainersResourceRequestPBImpl) request).getProto();
    request = new ChangeContainersResourceRequestPBImpl(proto);

    // check value
    Assert.assertEquals(request.getContainersToIncrease().size(),
        containerToIncrease.size());
    Assert.assertEquals(request.getContainersToDecrease().size(),
        containerToDecrease.size());
    for (int i = 0; i < containerToDecrease.size(); i++) {
      Assert.assertEquals(request.getContainersToDecrease().get(i),
          containerToDecrease.get(i));
    }
  }

  @Test
  public void testChangeContainersResourceRequestWithNull() {
    ChangeContainersResourceRequest request =
        ChangeContainersResourceRequest.newInstance(null, null);

    // serde
    ChangeContainersResourceRequestProto proto =
        ((ChangeContainersResourceRequestPBImpl) request).getProto();
    request = new ChangeContainersResourceRequestPBImpl(proto);

    // check value
    Assert.assertEquals(0, request.getContainersToIncrease().size());
    Assert.assertEquals(0, request.getContainersToDecrease().size());
  }
}
