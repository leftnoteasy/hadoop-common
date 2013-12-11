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
    ContainerId containerId =
        ContainerId.newInstance(ApplicationAttemptId.newInstance(
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

    ChangeContainersResourceResponse response =
        ChangeContainersResourceResponse.newInstance(succeedChanged,
            failChanged);

    // serde
    ChangeContainersResourceResponseProto proto =
        ((ChangeContainersResourceResponsePBImpl) response).getProto();
    response = new ChangeContainersResourceResponsePBImpl(proto);

    // check value
    Assert.assertEquals(response.getSucceedChangedContainers().size(),
        succeedChanged.size());
    Assert.assertEquals(response.getFailedChangedContainers().size(),
        failChanged.size());
    for (int i = 0; i < succeedChanged.size(); i++) {
      Assert.assertEquals(response.getSucceedChangedContainers().get(i),
          succeedChanged.get(i));
    }
    for (int i = 0; i < failChanged.size(); i++) {
      Assert.assertEquals(response.getFailedChangedContainers().get(i),
          failChanged.get(i));
    }
  }

  @Test
  public void testChangeContainersResourceRequestWithNull() {
    ChangeContainersResourceResponse request =
        ChangeContainersResourceResponse.newInstance(null, null);

    // serde
    ChangeContainersResourceResponseProto proto =
        ((ChangeContainersResourceResponsePBImpl) request).getProto();
    request = new ChangeContainersResourceResponsePBImpl(proto);

    // check value
    Assert.assertEquals(0, request.getSucceedChangedContainers().size());
    Assert.assertEquals(0, request.getFailedChangedContainers().size());
  }
}
