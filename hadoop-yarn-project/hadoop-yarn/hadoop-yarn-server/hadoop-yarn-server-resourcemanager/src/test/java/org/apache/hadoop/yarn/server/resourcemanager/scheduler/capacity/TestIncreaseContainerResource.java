package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.TestFifoScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Test;

public class TestIncreaseContainerResource {
  private static final Log LOG = LogFactory.getLog(TestFifoScheduler.class);

  private final int GB = 1024;

  @Test(timeout = 3000000)
  public void testIncreaseContainerResource() throws Exception {
    /*
     * This test case is pretty strait-forward, we allocate a container, then
     * increase resources for it
     */
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 3 * GB, 4);

    nm1.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(1, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // get existing container id
    ContainerId existingContainerId = checkAllocateResource(am1, nm1, "*", GB);
    Resource targetCapability = Resource.newInstance(2 * GB, 1);
    checkIncreaseResource(am1, nm1, existingContainerId, targetCapability);

    rm.stop();
  }

  @Test(timeout = 3000000)
  public void testReserveIncreaseContainerResource() throws Exception {
    /*
     * This test case is allocate two containers c_1 and c_2 in a node, no more
     * spaces on this node Then we will try to increase capa of c_1, this
     * increase request will be reserved and satisfied after c_2 released. We
     * will check queue's usedResource after each operation
     */
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 4);

    nm1.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(1, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // get existing container id
    ContainerId containerId1 = checkAllocateResource(am1, nm1, "*", GB);

    // do allocation for a 2G's container
    ContainerId containerId2 = checkAllocateResource(am1, nm1, "*", 2 * GB);
    
    // here we register another nm to increase cluster resource to make resource
    // can be reserved but can not allocated (I do it here because LeafQueue
    // will invoke "assignToQueue" to make sure this.
    MockNM nm2 = rm.registerNode("127.0.0.1:3399", 2 * GB, 1);
    nm2.nodeHeartbeat(true);

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(4 * GB, 3));

    // try to increase, but this will be failed
    Resource targetCapability = Resource.newInstance(3 * GB, 1);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability,
        false);

    // in this case, the increase will be reserved, we will check this
    FiCaSchedulerNode node = cs.getNode(nm1.getNodeId());
    Assert.assertTrue(node.isReserved());
    Assert.assertTrue(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(6 * GB, 3));

    // release containerId2, we should get this increase resource allocated
    am1.addContainerToBeReleased(containerId2);
    int waitCounter = 20;
    AllocateResponse alloc1Response = am1.schedule();
    LOG.info("heartbeating nm1");
    while (alloc1Response.getIncreasedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am1.schedule();
      nm1.nodeHeartbeat(true);
    }
    Assert.assertEquals(1, alloc1Response.getIncreasedContainers().size());
    ResourceIncreaseContext increasedContext = alloc1Response.getIncreasedContainers().get(0);
    checkIncreaseContext(increasedContext, targetCapability);
    checkQueueResource(cs, Resource.newInstance(4 * GB, 2));
    
    // release containerId1, we should free other resources
    am1.addContainerToBeReleased(containerId1);
    alloc1Response = am1.schedule();
    checkQueueResource(cs, Resource.newInstance(1 * GB, 1));

    rm.stop();
  }
  
  @Test(timeout = 3000000)
  public void testReleaseContainerWithReservedIncreaseRequest() throws Exception {
    /*
     * We will test a case that like the test above, we create two container c_1
     * and c_2, and let a increase request for c_1. This will be reserved like
     * above. Then we will release c_1, we will check if reserved spaces are freed
     */
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 4);

    nm1.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(1, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // get existing container id
    ContainerId containerId1 = checkAllocateResource(am1, nm1, "*", GB);

    // do allocation for a 2G's container
    ContainerId containerId2 = checkAllocateResource(am1, nm1, "*", 2 * GB);
    
    // here we register another nm to increase cluster resource to make resource
    // can be reserved but can not allocated (I do it here because LeafQueue
    // will invoke "assignToQueue" to make sure this.
    MockNM nm2 = rm.registerNode("127.0.0.1:3399", 2 * GB, 1);
    nm2.nodeHeartbeat(true);

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(4 * GB, 3));

    // try to increase, but this will be failed
    Resource targetCapability = Resource.newInstance(3 * GB, 1);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability,
        false);

    // in this case, the increase will be reserved, we will check this
    FiCaSchedulerNode node = cs.getNode(nm1.getNodeId());
    Assert.assertTrue(node.isReserved());
    Assert.assertTrue(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(6 * GB, 3));
    
    // release containerId1, we should free other resources
    am1.addContainerToBeReleased(containerId1);
    am1.schedule();
    checkQueueResource(cs, Resource.newInstance(3 * GB, 2));
    
    // we will free c_2 at last
    am1.addContainerToBeReleased(containerId2);
    am1.schedule();
    checkQueueResource(cs, Resource.newInstance(1 * GB, 1));
    
    rm.stop();
  }
  
  @Test(timeout = 3000000)
  public void testUpdateResourceIncreaseRequestForReserved() throws Exception {
    /*
     * We will test a test to see if our update resource increase request works,
     * 1) Create a reserved increase request in a node, this shouldn't be
     * satisfied 
     * 2) Submit a increase request that can be satisfied, see if it's
     * successfully increased
     */
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 4);

    nm1.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(1, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // get existing container id
    ContainerId containerId1 = checkAllocateResource(am1, nm1, "*", GB);
    
    // here we register another nm to increase cluster resource to make resource
    // can be reserved but can not allocated (I do it here because LeafQueue
    // will invoke "assignToQueue" to make sure this.
    MockNM nm2 = rm.registerNode("127.0.0.1:3399", 2 * GB, 1);
    nm2.nodeHeartbeat(true);

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(2 * GB, 2));

    // try to increase, but this will be failed
    Resource targetCapability = Resource.newInstance(5 * GB, 1);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability,
        false);

    // in this case, the increase will be reserved, we will check this
    FiCaSchedulerNode node = cs.getNode(nm1.getNodeId());
    Assert.assertTrue(node.isReserved());
    Assert.assertTrue(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(6 * GB, 2));
    
    // try to increase, and this should be succeed
    targetCapability = Resource.newInstance(4 * GB, 1);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability, true);
    checkQueueResource(cs, Resource.newInstance(5 * GB, 2));
    
    rm.stop();
  }
  
  @Test(timeout = 3000000)
  public void testUpdateResourceToCancelReservedResource() throws Exception {
    /*
     * We will test a test to see if our update resource increase request works,
     * 1) Create a reserved increase request in a node, this shouldn't be
     * satisfied 
     * 2) Submit a increase request that can be satisfied, see if it's
     * successfully increased
     */
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 4);

    nm1.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(1, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // get existing container id
    ContainerId containerId1 = checkAllocateResource(am1, nm1, "*", GB);
    
    // here we register another nm to increase cluster resource to make resource
    // can be reserved but can not allocated (I do it here because LeafQueue
    // will invoke "assignToQueue" to make sure this.
    MockNM nm2 = rm.registerNode("127.0.0.1:3399", 2 * GB, 1);
    nm2.nodeHeartbeat(true);

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(2 * GB, 2));

    // try to increase, but this will be failed
    Resource targetCapability = Resource.newInstance(5 * GB, 1);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability,
        false);

    // in this case, the increase will be reserved, we will check this
    FiCaSchedulerApp application = cs.getApplication(containerId1.getApplicationAttemptId());
    FiCaSchedulerNode node = cs.getNode(nm1.getNodeId());
    Assert.assertTrue(node.isReserved());
    Assert.assertTrue(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));
    Assert.assertNotNull(application.getResourceIncreaseRequest(node.getNodeID(), containerId1));

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(6 * GB, 2));
    
    // try to increase, and this should cancel previous request
    targetCapability = Resource.newInstance(1 * GB, 1);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability, false);
    checkQueueResource(cs, Resource.newInstance(2 * GB, 2));
    
    // and we will check if this request are removed from app/node
    Assert.assertFalse(node.isReserved());
    Assert.assertFalse(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));
    Assert.assertNull(application.getResourceIncreaseRequest(node.getNodeID(), containerId1));
    
    rm.stop();
  }
  
  @Test(timeout = 3000000)
  public void testSubmitInvalidIncreaseRequest() throws Exception {
    /*
     * We will test a test to see if our update resource increase request works,
     * 1) Create a reserved increase request in a node, this shouldn't be
     * satisfied 
     * 2) Submit a increase request that can be satisfied, see if it's
     * successfully increased
     */
    YarnConfiguration conf = new YarnConfiguration();
    MockRM rm = new MockRM(conf);
    rm.start();
    CapacityScheduler cs = (CapacityScheduler) rm.getResourceScheduler();

    // Register node1
    MockNM nm1 = rm.registerNode("127.0.0.1:1234", 5 * GB, 4);

    nm1.nodeHeartbeat(true);

    // wait..
    int waitCount = 20;
    int size = rm.getRMContext().getRMNodes().size();
    while ((size = rm.getRMContext().getRMNodes().size()) != 2
        && waitCount-- > 0) {
      LOG.info("Waiting for node managers to register : " + size);
      Thread.sleep(100);
    }
    Assert.assertEquals(1, rm.getRMContext().getRMNodes().size());
    // Submit an application
    RMApp app1 = rm.submitApp(128);

    // kick the scheduling
    nm1.nodeHeartbeat(true);
    RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
    MockAM am1 = rm.sendAMLaunched(attempt1.getAppAttemptId());
    am1.registerAppAttempt();

    // get existing container id
    ContainerId containerId1 = checkAllocateResource(am1, nm1, "*", GB);
    
    // here we register another nm to increase cluster resource to make resource
    // can be reserved but can not allocated (I do it here because LeafQueue
    // will invoke "assignToQueue" to make sure this.
    MockNM nm2 = rm.registerNode("127.0.0.1:3399", 2 * GB, 1);
    nm2.nodeHeartbeat(true);

    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(2 * GB, 2));

    // in this case, the increase will be reserved, we will check this
    FiCaSchedulerApp application = cs.getApplication(containerId1.getApplicationAttemptId());
    FiCaSchedulerNode node = cs.getNode(nm1.getNodeId());
    
    // and we will check if this request are removed from app/node
    Assert.assertFalse(node.isReserved());
    Assert.assertFalse(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));
    Assert.assertNull(application.getResourceIncreaseRequest(node.getNodeID(), containerId1));
    
    // try to increase, but this will be failed
    Resource targetCapability = Resource.newInstance(1 * GB, 0);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability,
        false);
    
    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(2 * GB, 2));
    
    // and we will check if this request are removed from app/node
    Assert.assertFalse(node.isReserved());
    Assert.assertFalse(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));
    Assert.assertNull(application.getResourceIncreaseRequest(node.getNodeID(), containerId1));
   
    // try to increase, but this will be failed
    targetCapability = Resource.newInstance(GB / 2, 1);
    checkIncreaseResource(am1, nm1, containerId1, targetCapability,
        false);
    
    // we will do a check on used resource here for queue
    checkQueueResource(cs, Resource.newInstance(2 * GB, 2));
    
    // and we will check if this request are removed from app/node
    Assert.assertFalse(node.isReserved());
    Assert.assertFalse(node.getReservedIncreaseRequest() != null
        && node.getReservedIncreaseRequest().getExistingContainerId()
            .equals(containerId1));
    Assert.assertNull(application.getResourceIncreaseRequest(node.getNodeID(), containerId1));
    
    rm.stop();
  }
  
  private ContainerId checkAllocateResource(MockAM am, MockNM nm,
      String resourceName, int memory) throws Exception {
    return checkAllocateResource(am, nm, resourceName, memory, true);
  }

  private void checkQueueResource(CapacityScheduler scheduler,
      Resource usedResource) {
    CSQueue root = scheduler.getRootQueue();
    Assert.assertTrue("not equal, expected=" + usedResource.toString()
        + " actual=" + root.getUsedResources().toString(),
        Resources.equals(usedResource, root.getUsedResources()));
    // assert child queue as well
    for (CSQueue child : root.getChildQueues()) {
      Assert.assertTrue("not equal, expected=" + usedResource.toString()
          + " actual=" + child.getUsedResources().toString(),
          Resources.equals(usedResource, child.getUsedResources()));
    }
  }

  private ContainerId checkAllocateResource(MockAM am, MockNM nm,
      String resourceName, int memory, boolean expectAllocated)
      throws Exception {
    LOG.info("sending container requests ");
    am.addRequests(new String[] { "*" }, memory, 1, 1);
    AllocateResponse alloc1Response = am.schedule(); // send the request

    // kick the scheduler
    nm.nodeHeartbeat(true);
    int waitCounter = 20;
    LOG.info("heartbeating nm1");
    while (alloc1Response.getAllocatedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waiting for containers to be created for app 1...");
      Thread.sleep(500);
      alloc1Response = am.schedule();
    }
    LOG.info("received container : "
        + alloc1Response.getAllocatedContainers().size());

    // There should be one container allocated
    // Internally it should not been reserved.
    if (expectAllocated) {
      Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 1);

      // get existing container id
      ContainerId containerId = alloc1Response.getAllocatedContainers().get(0)
          .getId();

      return containerId;
    } else {
      Assert.assertTrue(alloc1Response.getAllocatedContainers().size() == 0);
      return null;
    }
  }

  private void checkIncreaseResource(MockAM am, MockNM nm,
      ContainerId containerId, Resource targetResource) throws Exception {
    checkIncreaseResource(am, nm, containerId, targetResource, true);
  }

  private void checkIncreaseResource(MockAM am, MockNM nm,
      ContainerId containerId, Resource targetResource, boolean expectIncreased)
      throws Exception {
    // add increase request
    am.addIncreaseRequest(containerId, targetResource);
    AllocateResponse alloc1Response = am.schedule();

    // kick the scheduling again
    nm.nodeHeartbeat(true);
    int waitCounter = 20;
    LOG.info("heartbeating nm1");
    while (alloc1Response.getIncreasedContainers().size() < 1
        && waitCounter-- > 0) {
      LOG.info("Waitting for containers to be increased for app 1...");
      Thread.sleep(500);
      alloc1Response = am.schedule();
    }
    LOG.info("received increased container:"
        + alloc1Response.getIncreasedContainers().size());

    if (expectIncreased) {
      Assert.assertEquals(1, alloc1Response.getIncreasedContainers().size());
      ResourceIncreaseContext increasedContainer = alloc1Response
          .getIncreasedContainers().get(0);
      checkIncreaseContext(increasedContainer, targetResource); 
    } else {
      Assert.assertTrue(0 == alloc1Response.getIncreasedContainers().size());
    }
  }

  private void checkIncreaseContext(ResourceIncreaseContext increaseContext,
      Resource expectedResource) throws Exception {
    Assert.assertEquals(expectedResource, increaseContext
        .getResourceChangeContext().getTargetCapability());

    // check token received
    ContainerTokenIdentifier ti = BuilderUtils
        .newContainerTokenIdentifier(increaseContext.getContainerToken());
    Assert.assertEquals(expectedResource, ti.getResource());
  }
}
