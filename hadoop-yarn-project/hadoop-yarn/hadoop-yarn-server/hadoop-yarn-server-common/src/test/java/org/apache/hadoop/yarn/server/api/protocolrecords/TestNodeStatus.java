package org.apache.hadoop.yarn.server.api.protocolrecords;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
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
    List<ResourceChangeContext> list = new ArrayList<ResourceChangeContext>();
    list.add(ResourceChangeContext.newInstance(null, null));
    list.add(ResourceChangeContext.newInstance(null, null));
    list.add(ResourceChangeContext.newInstance(null, null));
    nodeStatus.setNewDecreasedContainers(list);
    
    // serde
    NodeStatusProto proto = ((NodeStatusPBImpl)nodeStatus).getProto();
    nodeStatus = new NodeStatusPBImpl(proto);
    
    // check value
    Assert.assertEquals(list.size(), nodeStatus.getNewsDecreasedContainers().size());
  }
  
  @Test
  public void testNodeStatusWithoutDecreasedContainers() {
    RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    NodeStatus nodeStatus = recordFactory.newRecordInstance(NodeStatus.class);
    
    // serde
    NodeStatusProto proto = ((NodeStatusPBImpl)nodeStatus).getProto();
    nodeStatus = new NodeStatusPBImpl(proto);
    
    // check value
    Assert.assertEquals(0, nodeStatus.getNewsDecreasedContainers().size());
  }
}
