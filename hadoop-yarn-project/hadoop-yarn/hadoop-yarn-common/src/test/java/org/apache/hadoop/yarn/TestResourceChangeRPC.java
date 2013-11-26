package org.apache.hadoop.yarn;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceRequest;
import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceResponse;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.ipc.HadoopYarnProtoRPC;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.junit.Test;

public class TestResourceChangeRPC {

  static final Log LOG = LogFactory.getLog(TestContainerLaunchRPC.class);

  @Test
  public void testResourceChangeRPC() throws Exception {
    testResourceChange(HadoopYarnProtoRPC.class.getName());
  }

  private void testResourceChange(String rpcClass) throws Exception {
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.IPC_RPC_IMPL, rpcClass);
    YarnRPC rpc = YarnRPC.create(conf);
    String bindAddr = "localhost:0";
    InetSocketAddress addr = NetUtils.createSocketAddr(bindAddr);
    Server server = rpc.getServer(ContainerManagementProtocol.class,
        new DummyContainerManager(), addr, conf, null, 1);
    server.start();
    try {

      ContainerManagementProtocol proxy = (ContainerManagementProtocol) rpc
          .getProxy(ContainerManagementProtocol.class,
              server.getListenerAddress(), conf);

      List<ResourceIncreaseContext> inc = new ArrayList<ResourceIncreaseContext>();
      List<ResourceChangeContext> dec = new ArrayList<ResourceChangeContext>();

      for (int i = 0; i < 3; i++) {
        inc.add(ResourceIncreaseContext.newInstance(null, null));
      }
      for (int i = 0; i < 5; i++) {
        dec.add(ResourceChangeContext.newInstance(null, null));
      }

      ChangeContainersResourceRequest req = ChangeContainersResourceRequest
          .newInstance(inc, dec);

      try {
        ChangeContainersResourceResponse res = proxy
            .changeContainersResource(req);
        Assert.assertEquals(inc.size(), res.getSucceedChangedContainers()
            .size());
        Assert.assertEquals(dec.size(), res.getFailedChangedContainers()
            .size());
      } catch (Exception e) {
        e.printStackTrace();
        Assert.fail(e.getMessage());
      }
    } finally {
      server.stop();
    }
  }

  public class DummyContainerManager implements ContainerManagementProtocol {

    @Override
    public StartContainersResponse startContainers(
        StartContainersRequest requests) throws YarnException, IOException {
      return null;
    }

    @Override
    public StopContainersResponse stopContainers(StopContainersRequest requests)
        throws YarnException, IOException {
      return null;
    }

    @Override
    public GetContainerStatusesResponse getContainerStatuses(
        GetContainerStatusesRequest request) throws YarnException, IOException {
      return null;
    }

    @Override
    public ChangeContainersResourceResponse changeContainersResource(
        ChangeContainersResourceRequest request) throws YarnException,
        IOException {
      ContainerId cid = ContainerId.newInstance(null, 0);
      List<ContainerId> inc = new ArrayList<ContainerId>();
      List<ContainerId> dec = new ArrayList<ContainerId>();
      for (int i = 0; i < request.getContainersToIncrease().size(); i++) {
        inc.add(cid);
      }
      for (int i = 0; i < request.getContainersToDecrease().size(); i++) {
        dec.add(cid);
      }

      ChangeContainersResourceResponse res = ChangeContainersResourceResponse
          .newInstance(inc, dec);
      return res;
    }
  }
}
