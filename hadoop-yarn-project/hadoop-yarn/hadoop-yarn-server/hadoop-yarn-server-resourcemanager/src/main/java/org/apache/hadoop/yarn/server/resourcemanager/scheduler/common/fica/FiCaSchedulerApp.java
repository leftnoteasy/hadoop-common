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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerReservedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AppSchedulingInfo;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Represents an application attempt from the viewpoint of the FIFO or Capacity
 * scheduler.
 */
@Private
@Unstable
public class FiCaSchedulerApp extends SchedulerApplication {

  private static final Log LOG = LogFactory.getLog(FiCaSchedulerApp.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  private final Resource currentConsumption = recordFactory
      .newRecordInstance(Resource.class);
  private Resource resourceLimit = recordFactory
      .newRecordInstance(Resource.class);

  private Map<ContainerId, RMContainer> liveContainers =
    new HashMap<ContainerId, RMContainer>();
  private List<RMContainer> newlyAllocatedContainers =
    new ArrayList<RMContainer>();
  private List<Container> newlyIncreasedContainers =
    new ArrayList<Container>();
  private List<Container> newlyDecreasedContainers = 
    new ArrayList<Container>();

  final Map<Priority, Map<NodeId, RMContainer>> reservedContainers = 
      new HashMap<Priority, Map<NodeId, RMContainer>>();

  private boolean isStopped = false;

  private final Set<ContainerId> containersToPreempt =
    new HashSet<ContainerId>();
  
  public FiCaSchedulerApp(ApplicationAttemptId applicationAttemptId, 
      String user, Queue queue, ActiveUsersManager activeUsersManager,
      RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
  }

  synchronized public boolean containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {

    // Remove from the list of containers
    if (null == liveContainers.remove(rmContainer.getContainerId())) {
      return false;
    }

    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();

    // Inform the container
    rmContainer.handle(
        new RMContainerFinishedEvent(
            containerId,
            containerStatus, 
            event)
        );
    LOG.info("Completed container: " + rmContainer.getContainerId() + 
        " in state: " + rmContainer.getState() + " event:" + event);

    containersToPreempt.remove(rmContainer.getContainerId());

    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.RELEASE_CONTAINER, "SchedulerApp", 
        getApplicationId(), containerId);
    
    // Update usage metrics 
    Resource containerResource = rmContainer.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);

    return true;
  }
  
  synchronized public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      Priority priority, ResourceRequest request, 
      Container container) {
    return allocate(type, node, priority, request, container, null);
  }

  synchronized public RMContainer allocate(NodeType type, FiCaSchedulerNode node,
      Priority priority, ResourceRequest request, 
      Container container, RMContainer existingContainer) {

    if (isStopped) {
      return null;
    }
    
    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }
    
    if (existingContainer == null) {
      // We're allocate on an existing container, create RMContainer.
      RMContainer rmContainer = new RMContainerImpl(container, this
          .getApplicationAttemptId(), node.getNodeID(), this.rmContext
          .getDispatcher().getEventHandler(), this.rmContext
          .getContainerAllocationExpirer());
  
      // Add it to allContainers list.
      newlyAllocatedContainers.add(rmContainer);
      liveContainers.put(container.getId(), rmContainer);    
  
      // Update consumption and track allocations
      appSchedulingInfo.allocate(type, node, priority, request, container);
      Resources.addTo(currentConsumption, container.getResource());
  
      // Inform the container
      rmContainer.handle(
          new RMContainerEvent(container.getId(), RMContainerEventType.START));
  
      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate: applicationAttemptId=" 
            + container.getId().getApplicationAttemptId() 
            + " container=" + container.getId() + " host="
            + container.getNodeId().getHost() + " type=" + type);
      }
      RMAuditLogger.logSuccess(getUser(), 
          AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
          getApplicationId(), container.getId());
      
      return rmContainer;
    } else {
      newlyIncreasedContainers.add(container);
      appSchedulingInfo.allocate(type, node, priority, request, container);
      // add increment to current comsuption
      Resources.addTo(currentConsumption, request.getCapability());
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("allocate on existing container: applicationAttemptId="
            + container.getId().getApplicationAttemptId() + " container="
            + container.getId() + " host=" + container.getNodeId().getHost()
            + " type=" + type + " allocated:" + request.getCapability()
            + " size after allocated:" + container.getResource());
      }
      RMAuditLogger.logSuccess(getUser(), 
          AuditConstants.ALLOC_EXISTING_CONTAINER, "SchedulerApp", 
          getApplicationId(), container.getId());      
      return existingContainer;
    }
  }
  
  synchronized public void removeIncreasingRequest(String resourceName, ContainerId containerId) {
    appSchedulingInfo.removeIncreasingRequest(resourceName, containerId);
  }
  
  synchronized public List<Container> pullNewlyAllocatedContainers() {
    List<Container> returnContainerList = new ArrayList<Container>(
        newlyAllocatedContainers.size());
    for (RMContainer rmContainer : newlyAllocatedContainers) {
      rmContainer.handle(new RMContainerEvent(rmContainer.getContainerId(),
          RMContainerEventType.ACQUIRED));
      returnContainerList.add(rmContainer.getContainer());
    }
    newlyAllocatedContainers.clear();
    return returnContainerList;
  }
  
  synchronized public List<Container> pullNewlyIncreasedContainers() {
    List<Container> returnList = new ArrayList<Container>();
    returnList.addAll(newlyIncreasedContainers);
    newlyIncreasedContainers.clear();
    return returnList;
  }
  
  synchronized public List<Container> pullNewlyDecreasedContainers() {
    List<Container> returnList = new ArrayList<Container>();
    returnList.addAll(newlyDecreasedContainers);
    newlyDecreasedContainers.clear();
    return returnList;
  }

  public Resource getCurrentConsumption() {
    return this.currentConsumption;
  }

  synchronized public void showRequests() {
    if (LOG.isDebugEnabled()) {
      for (Priority priority : getPriorities()) {
        Map<String, ResourceRequest> requests = getResourceRequests(priority);
        if (requests != null) {
          LOG.debug("showRequests:" + " application=" + getApplicationId() + 
              " headRoom=" + getHeadroom() + 
              " currentConsumption=" + currentConsumption.getMemory());
          for (ResourceRequest request : requests.values()) {
            LOG.debug("showRequests:" + " application=" + getApplicationId()
                + " request=" + request);
          }
        }
      }
    }
  }

  public synchronized RMContainer getRMContainer(ContainerId id) {
    return liveContainers.get(id);
  }

  public synchronized int getNumReservedContainers(Priority priority) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    return (reservedContainers == null) ? 0 : reservedContainers.size();
  }
  
  /**
   * Get total current reservations.
   * Used only by unit tests
   * @return total current reservations
   */
  @Stable
  @Private
  public synchronized Resource getCurrentReservation() {
    return currentReservation;
  }
  
  public synchronized RMContainer reserve(FiCaSchedulerNode node,
      Priority priority, RMContainer rmContainer, Container container) {
    return reserve(node, priority, rmContainer, container, null);
  }

  public synchronized RMContainer reserve(FiCaSchedulerNode node,
      Priority priority, RMContainer rmContainer, Container container,
      Resource reservedResource) {
    // Create RMContainer if necessary
    if (rmContainer == null) {
      rmContainer = 
          new RMContainerImpl(container, getApplicationAttemptId(), 
              node.getNodeID(), rmContext.getDispatcher().getEventHandler(), 
              rmContext.getContainerAllocationExpirer());
        
      Resources.addTo(currentReservation, container.getResource());
      
      // Reset the re-reservation count
      resetReReservations(priority);
    } else {
      // Note down the re-reservation
      addReReservation(priority);
    }

    rmContainer.handle(new RMContainerReservedEvent(container.getId(),
        reservedResource, node.getNodeID(), priority));
    
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    if (reservedContainers == null) {
      reservedContainers = new HashMap<NodeId, RMContainer>();
      this.reservedContainers.put(priority, reservedContainers);
    }
    reservedContainers.put(node.getNodeID(), rmContainer);
    
    LOG.info("Application " + getApplicationId() 
        + " reserved container " + rmContainer
        + " on node " + node + ", currently has " + reservedContainers.size()
        + " at priority " + priority 
        + "; currentReservation " + currentReservation.getMemory());
    
    return rmContainer;
  }

  public synchronized boolean unreserve(FiCaSchedulerNode node, Priority priority) {
    Map<NodeId, RMContainer> reservedContainers =
      this.reservedContainers.get(priority);

    if (reservedContainers != null) {
      RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());

      // unreserve is now triggered in new scenarios (preemption)
      // as a consequence reservedcontainer might be null, adding NP-checks
      if (reservedContainer != null
          && reservedContainer.getContainer() != null
          && reservedContainer.getContainer().getResource() != null) {

        if (reservedContainers.isEmpty()) {
          this.reservedContainers.remove(priority);
        }
        // Reset the re-reservation count
        resetReReservations(priority);

        Resource resource = reservedContainer.getContainer().getResource();
        Resources.subtractFrom(currentReservation, resource);

        LOG.info("Application " + getApplicationId() + " unreserved "
            + " on node " + node + ", currently has " + reservedContainers.size()
            + " at priority " + priority + "; currentReservation "
            + currentReservation);
        return true;
      }
    }
    return false;
  }

  public synchronized float getLocalityWaitFactor(
      Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = 
        Math.max(this.getResourceRequests(priority).size() - 1, 0);
    
    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }

  public Resource getTotalPendingRequests() {
    Resource ret = Resource.newInstance(0, 0);
    for (ResourceRequest rr : appSchedulingInfo.getAllResourceRequests()) {
      // to avoid double counting we count only "ANY" resource requests
      if (ResourceRequest.isAnyLocation(rr.getResourceName())){
        Resources.addTo(ret,
            Resources.multiply(rr.getCapability(), rr.getNumContainers()));
      }
    }
    return ret;
  }

  public synchronized void addPreemptContainer(ContainerId cont){
    // ignore already completed containers
    if (liveContainers.containsKey(cont)) {
      containersToPreempt.add(cont);
    }
  }

  /**
   * This method produces an Allocation that includes the current view
   * of the resources that will be allocated to and preempted from this
   * application.
   *
   * @param rc
   * @param clusterResource
   * @param minimumAllocation
   * @return an allocation
   */
  public synchronized Allocation getAllocation(ResourceCalculator rc,
      Resource clusterResource, Resource minimumAllocation) {

    Set<ContainerId> currentContPreemption = Collections.unmodifiableSet(
        new HashSet<ContainerId>(containersToPreempt));
    containersToPreempt.clear();
    Resource tot = Resource.newInstance(0, 0);
    for(ContainerId c : currentContPreemption){
      Resources.addTo(tot,
          liveContainers.get(c).getContainer().getResource());
    }
    int numCont = (int) Math.ceil(
        Resources.divide(rc, clusterResource, tot, minimumAllocation));
    ResourceRequest rr = ResourceRequest.newInstance(
        Priority.UNDEFINED, ResourceRequest.ANY,
        minimumAllocation, numCont);
    return new Allocation(pullNewlyAllocatedContainers(), getHeadroom(),
                          null, currentContPreemption,
                          Collections.singletonList(rr));
  }

}
