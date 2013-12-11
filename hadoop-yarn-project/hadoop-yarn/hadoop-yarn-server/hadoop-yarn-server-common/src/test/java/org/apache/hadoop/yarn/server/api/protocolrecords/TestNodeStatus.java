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

package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.NodeStatusProto;
import org.apache.hadoop.yarn.server.api.records.NodeStatus;
import org.apache.hadoop.yarn.server.api.records.impl.pb.NodeStatusPBImpl;
import org.junit.Test;

public class TestNodeStatus {
  @Test
  public void testNodeStatusWithDecreasedContainers() {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    NodeStatus nodeStatus = recordFactory.newRecordInstance(NodeStatus.class);

    // add 3 instances
    List<ContainerResourceDecrease> list =
        new ArrayList<ContainerResourceDecrease>();
    list.add(ContainerResourceDecrease.newInstance(null, null));
    list.add(ContainerResourceDecrease.newInstance(null, null));
    list.add(ContainerResourceDecrease.newInstance(null, null));
    nodeStatus.setDecreasedContainers(list);

    // serde
    NodeStatusProto proto = ((NodeStatusPBImpl) nodeStatus).getProto();
    nodeStatus = new NodeStatusPBImpl(proto);

    // check value
    Assert.assertEquals(list.size(), nodeStatus.getDecreasedContainers()
        .size());
  }

  @Test
  public void testNodeStatusWithoutDecreasedContainers() {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    NodeStatus nodeStatus = recordFactory.newRecordInstance(NodeStatus.class);

    // serde
    NodeStatusProto proto = ((NodeStatusPBImpl) nodeStatus).getProto();
    nodeStatus = new NodeStatusPBImpl(proto);

    // check value
    Assert.assertEquals(0, nodeStatus.getDecreasedContainers().size());
  }
}
