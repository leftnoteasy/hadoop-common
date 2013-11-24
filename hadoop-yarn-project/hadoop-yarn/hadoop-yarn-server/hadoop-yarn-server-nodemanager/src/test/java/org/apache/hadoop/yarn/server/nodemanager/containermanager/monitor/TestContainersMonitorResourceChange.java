package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl.ProcessTreeInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestContainersMonitorResourceChange {
  ContainersMonitorImpl cm;
  MockExecutor executor;
  Configuration conf;
  AsyncDispatcher dispatcher;
  MockContainerEventHandler containerEventHandler;

  static class MockExecutor extends ContainerExecutor {
    ConcurrentMap<ContainerId, String> containerIdToPid = new ConcurrentHashMap<ContainerId, String>();

    @Override
    public void init() throws IOException {
    }

    @Override
    public void startLocalizer(Path nmPrivateContainerTokens,
        InetSocketAddress nmAddr, String user, String appId, String locId,
        List<String> localDirs, List<String> logDirs) throws IOException,
        InterruptedException {
    }

    @Override
    public int launchContainer(Container container,
        Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
        String user, String appId, Path containerWorkDir,
        List<String> localDirs, List<String> logDirs) throws IOException {
      return 0;
    }

    @Override
    public boolean signalContainer(String user, String pid, Signal signal)
        throws IOException {
      return true;
    }

    @Override
    public void deleteAsUser(String user, Path subDir, Path... basedirs)
        throws IOException, InterruptedException {
    }

    @Override
    public String getProcessId(ContainerId containerID) {
      return String.valueOf(containerID.getId());
    }

    public void setContainerPid(ContainerId containerId, String pid) {
      containerIdToPid.put(containerId, pid);
    }
  }

  static class MockContainerEventHandler implements
      EventHandler<ContainerEvent> {
    Set<ContainerId> killedContainer = new HashSet<ContainerId>();

    @Override
    public void handle(ContainerEvent event) {
      if (event.getType() == ContainerEventType.KILL_CONTAINER) {
        synchronized (killedContainer) {
          killedContainer.add(event.getContainerID());
        }
      }
    }

    public boolean isContainerKilled(ContainerId containerId) {
      synchronized (killedContainer) {
        return killedContainer.contains(containerId);
      }
    }
  }

  @Before
  public void setup() {
    executor = new MockExecutor();
    dispatcher = new AsyncDispatcher();
    conf = new Configuration();
    conf.set(
        YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
        "org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.MockResourceCalculatorPlugin");
    conf.set(
        YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE,
        "org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.MockResourceCalculatorProcessTree");
    conf.setLong(YarnConfiguration.NM_CONTAINER_MON_INTERVAL_MS, 20L);
    dispatcher.init(conf);
    dispatcher.start();
    containerEventHandler = new MockContainerEventHandler();
    dispatcher.register(ContainerEventType.class, containerEventHandler);
    cm = new ContainersMonitorImpl(executor, dispatcher, null);
    cm.init(conf);
    cm.start();
  }

  @After
  public void finalize() {
    try {
      cm.stop();
    } catch (Exception e) {
      // do nothing
    }
    try {
      dispatcher.stop();
    } catch (Exception e) {
      // do nothing
    }
  }

  private ContainerId getContainerId(int id) {
    return ContainerId.newInstance(ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(123456L, 1), 1), id);
  }

  private ProcessTreeInfo getProcessTreeInfo(ContainerId id) {
    return cm.trackingContainers.get(id);
  }

  @Test
  public void testResourceChange() throws Exception {
    // create a container-1 with
    cm.handle(new ContainerStartMonitoringEvent(getContainerId(1), 2100L, 1000L));

    // check if this container is tracked and it's value
    Thread.sleep(200);
    Assert.assertNotNull(getProcessTreeInfo(getContainerId(1)));
    Assert.assertEquals(1000L, getProcessTreeInfo(getContainerId(1))
        .getPmemLimit());
    Assert.assertEquals(2100L, getProcessTreeInfo(getContainerId(1))
        .getVmemLimit());

    // increase size of pmem usage, it will be killed
    MockResourceCalculatorProcessTree mockTree = (MockResourceCalculatorProcessTree) getProcessTreeInfo(
        getContainerId(1)).getProcessTree();
    mockTree.setPmem(2500L);

    // check if this container killed
    Thread.sleep(200);
    Assert.assertTrue(containerEventHandler
        .isContainerKilled(getContainerId(1)));
    
    // create a container-2 with
    cm.handle(new ContainerStartMonitoringEvent(getContainerId(2), 2100L, 1000L));

    // check if this container is tracked and it's value
    Thread.sleep(200);
    Assert.assertNotNull(getProcessTreeInfo(getContainerId(2)));
    Assert.assertEquals(1000L, getProcessTreeInfo(getContainerId(2))
        .getPmemLimit());
    Assert.assertEquals(2100L, getProcessTreeInfo(getContainerId(2))
        .getVmemLimit());
    
    // trigger a change resource event, check limit after changed
    cm.handle(new ContainerChangeMonitoringEvent(getContainerId(2), 4200L, 2000L));
    Thread.sleep(200);
    Assert.assertNotNull(getProcessTreeInfo(getContainerId(2)));
    Assert.assertEquals(2000L, getProcessTreeInfo(getContainerId(2))
        .getPmemLimit());
    Assert.assertEquals(4200L, getProcessTreeInfo(getContainerId(2))
        .getVmemLimit());
    
    // increase size of pmem usage, it should NOT be killed
    mockTree = (MockResourceCalculatorProcessTree) getProcessTreeInfo(
        getContainerId(2)).getProcessTree();
    mockTree.setPmem(2500L);
    
    // check if this container killed
    Thread.sleep(200);
    Assert.assertFalse(containerEventHandler
        .isContainerKilled(getContainerId(2)));
  }
}
