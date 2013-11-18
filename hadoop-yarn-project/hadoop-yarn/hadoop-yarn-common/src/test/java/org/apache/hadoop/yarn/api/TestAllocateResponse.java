package org.apache.hadoop.yarn.api;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.AllocateResponsePBImpl;
import org.apache.hadoop.yarn.api.records.AMCommand;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.AllocateResponseProto;
import org.junit.Test;

public class TestAllocateResponse {
  @Test
  public void testAllocateResponseWithIncDecContainers() {
    List<ResourceIncreaseContext> incContainers = new ArrayList<ResourceIncreaseContext>();
    List<ResourceChangeContext> decContainers = new ArrayList<ResourceChangeContext>();
    for (int i = 0; i < 3; i++) {
      incContainers
          .add(ResourceIncreaseContext.newInstance(
              ResourceChangeContext.newInstance(null,
                  Resource.newInstance(1024, i)), null));
    }
    for (int i = 0; i < 5; i++) {
      decContainers
          .add(ResourceChangeContext.newInstance(null,
              Resource.newInstance(1024, i)));
    }

    AllocateResponse r = AllocateResponse.newInstance(3,
        new ArrayList<ContainerStatus>(), new ArrayList<Container>(),
        new ArrayList<NodeReport>(), null, AMCommand.AM_RESYNC, 3, null,
        new ArrayList<NMToken>(), incContainers, decContainers);

    // serde
    AllocateResponseProto p = ((AllocateResponsePBImpl) r).getProto();
    r = new AllocateResponsePBImpl(p);

    // check value
    Assert
        .assertEquals(incContainers.size(), r.getIncreasedContainers().size());
    Assert
        .assertEquals(decContainers.size(), r.getDecreasedContainers().size());
    
    for (int i = 0; i < incContainers.size(); i++) {
      Assert.assertEquals(i, r.getIncreasedContainers().get(i)
          .getResourceChangeContext().getTargetCapability().getVirtualCores());
    }
    
    for (int i = 0; i < decContainers.size(); i++) {
      Assert.assertEquals(i, r.getDecreasedContainers().get(i)
          .getTargetCapability().getVirtualCores());
    }
  }
  
  @Test
  public void testAllocateResponseWithoutIncDecContainers() {
    AllocateResponse r = AllocateResponse.newInstance(3, new ArrayList<ContainerStatus>(), new ArrayList<Container>(), new ArrayList<NodeReport>(),
        null, AMCommand.AM_RESYNC, 3, null, new ArrayList<NMToken>(), null, null);
    
    // serde
    AllocateResponseProto p = ((AllocateResponsePBImpl)r).getProto();
    r = new AllocateResponsePBImpl(p);
    
    // check value
    Assert.assertEquals(0, r.getIncreasedContainers().size());
    Assert.assertEquals(0, r.getDecreasedContainers().size());
  }
}
