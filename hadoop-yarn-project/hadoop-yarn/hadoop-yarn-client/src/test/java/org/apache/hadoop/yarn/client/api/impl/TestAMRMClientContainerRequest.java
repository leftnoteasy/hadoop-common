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

package org.apache.hadoop.yarn.client.api.impl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.net.DNSToSwitchMapping;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.InvalidContainerRequestException;
import org.junit.Test;

public class TestAMRMClientContainerRequest {
  @Test
  public void testFillInRacks() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
 
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            new String[] {"/rack2"}, Priority.newInstance(1));
    client.addContainerRequest(request);
    verifyResourceRequest(client, request, "host1", true);
    verifyResourceRequest(client, request, "host2", true);
    verifyResourceRequest(client, request, "/rack1", true);
    verifyResourceRequest(client, request, "/rack2", true);
    verifyResourceRequest(client, request, ResourceRequest.ANY, true);
  }
  
  @Test
  public void testDisableLocalityRelaxation() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest nodeLevelRequest =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(nodeLevelRequest);

    verifyResourceRequest(client, nodeLevelRequest, ResourceRequest.ANY, false);
    verifyResourceRequest(client, nodeLevelRequest, "/rack1", false);
    verifyResourceRequest(client, nodeLevelRequest, "host1", true);
    verifyResourceRequest(client, nodeLevelRequest, "host2", true);
    
    // Make sure we don't get any errors with two node-level requests at the
    // same priority
    ContainerRequest nodeLevelRequest2 =
        new ContainerRequest(capability, new String[] {"host2", "host3"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(nodeLevelRequest2);
    
    AMRMClient.ContainerRequest rackLevelRequest =
        new AMRMClient.ContainerRequest(capability, null,
            new String[] {"/rack3", "/rack4"}, Priority.newInstance(2), false);
    client.addContainerRequest(rackLevelRequest);
    
    verifyResourceRequest(client, rackLevelRequest, ResourceRequest.ANY, false);
    verifyResourceRequest(client, rackLevelRequest, "/rack3", true);
    verifyResourceRequest(client, rackLevelRequest, "/rack4", true);
    
    // Make sure we don't get any errors with two rack-level requests at the
    // same priority
    AMRMClient.ContainerRequest rackLevelRequest2 =
        new AMRMClient.ContainerRequest(capability, null,
            new String[] {"/rack4", "/rack5"}, Priority.newInstance(2), false);
    client.addContainerRequest(rackLevelRequest2);
    
    ContainerRequest bothLevelRequest =
        new ContainerRequest(capability, new String[] {"host3", "host4"},
            new String[] {"rack1", "/otherrack"},
            Priority.newInstance(3), false);
    client.addContainerRequest(bothLevelRequest);

    verifyResourceRequest(client, bothLevelRequest, ResourceRequest.ANY, false);
    verifyResourceRequest(client, bothLevelRequest, "rack1",
        true);
    verifyResourceRequest(client, bothLevelRequest, "/otherrack",
        true);
    verifyResourceRequest(client, bothLevelRequest, "host3", true);
    verifyResourceRequest(client, bothLevelRequest, "host4", true);
    
    // Make sure we don't get any errors with two both-level requests at the
    // same priority
    ContainerRequest bothLevelRequest2 =
        new ContainerRequest(capability, new String[] {"host4", "host5"},
            new String[] {"rack1", "/otherrack2"},
            Priority.newInstance(3), false);
    client.addContainerRequest(bothLevelRequest2);
  }
  
  @Test (expected = NullPointerException.class)
  public void testFillIncreasingRequests() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request =
        new ContainerRequest(capability, new String[] {"host1"},
            null, Priority.newInstance(1), false, getContainerId(0));
    client.addContainerRequest(request);

    // we should have no problem in this request, only host level has container,
    // rack and ANY is not relaxation
    verifyResourceRequest(client, request, "host1", 1, true, getContainerId(0));
    verifyResourceRequest(client, request, "/rack1", 1, false, null);
    verifyResourceRequest(client, request, ResourceRequest.ANY, 1, false, null);
    
    // add another request in the host, with different capability, should no error
    capability = Resource.newInstance(2048, 1);
    request =
        new ContainerRequest(capability, new String[] {"host1"},
            null, Priority.newInstance(1), false, getContainerId(1));
    client.addContainerRequest(request);
    verifyResourceRequest(client, request, "host1", 1, true, getContainerId(1));
    verifyResourceRequest(client, request, "/rack1", 1, false, null);
    verifyResourceRequest(client, request, ResourceRequest.ANY, 1, false, null);

    // add a request with same capability in different host, host1/rack1 should
    // increase number of containers
    capability = Resource.newInstance(2048, 1);
    request =
        new ContainerRequest(capability, new String[] {"host2"},
            null, Priority.newInstance(1), false, getContainerId(2));
    client.addContainerRequest(request);
    verifyResourceRequest(client, request, "host2", 1, true, getContainerId(2));
    verifyResourceRequest(client, request, "/rack1", 2, false, null);
    verifyResourceRequest(client, request, ResourceRequest.ANY, 2, false, null);

    // add a request with same container id and same host, it should overwrite
    // existing container
    capability = Resource.newInstance(3072, 1);
    ContainerRequest requestOverwrite =
        new ContainerRequest(capability, new String[] {"host2"},
            null, Priority.newInstance(1), false, getContainerId(2));
    client.addContainerRequest(requestOverwrite);
    verifyResourceRequest(client, requestOverwrite, "host2", 1, true, getContainerId(2));
    verifyResourceRequest(client, requestOverwrite, "/rack1", 1, false, null);
    verifyResourceRequest(client, requestOverwrite, ResourceRequest.ANY, 1, false, null);
    
    // the old request will be removed (get NullPointerException when querying)
    verifyResourceRequest(client, request, "host2", 1, true, getContainerId(2));
  }
  
  @Test (expected = InvalidContainerRequestException.class)
  public void testDifferentLocalityRelaxationSamePriority() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request1 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request1);
    ContainerRequest request2 =
        new ContainerRequest(capability, new String[] {"host3"},
            null, Priority.newInstance(1), true);
    client.addContainerRequest(request2);
  }
  
  @Test
  public void testInvalidValidWhenOldRemoved() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request1 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request1);
    
    client.removeContainerRequest(request1);

    ContainerRequest request2 =
        new ContainerRequest(capability, new String[] {"host3"},
            null, Priority.newInstance(1), true);
    client.addContainerRequest(request2);
    
    client.removeContainerRequest(request2);
    
    ContainerRequest request3 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request3);
    
    client.removeContainerRequest(request3);
    
    ContainerRequest request4 =
        new ContainerRequest(capability, null,
            new String[] {"rack1"}, Priority.newInstance(1), true);
    client.addContainerRequest(request4);

  }
  
  @Test (expected = InvalidContainerRequestException.class)
  public void testLocalityRelaxationDifferentLevels() {
    AMRMClientImpl<ContainerRequest> client =
        new AMRMClientImpl<ContainerRequest>();
    Configuration conf = new Configuration();
    conf.setClass(
        CommonConfigurationKeysPublic.NET_TOPOLOGY_NODE_SWITCH_MAPPING_IMPL_KEY,
        MyResolver.class, DNSToSwitchMapping.class);
    client.init(conf);
    
    Resource capability = Resource.newInstance(1024, 1);
    ContainerRequest request1 =
        new ContainerRequest(capability, new String[] {"host1", "host2"},
            null, Priority.newInstance(1), false);
    client.addContainerRequest(request1);
    ContainerRequest request2 =
        new ContainerRequest(capability, null,
            new String[] {"rack1"}, Priority.newInstance(1), true);
    client.addContainerRequest(request2);
  }
  
  @Test (expected = IllegalArgumentException.class) 
  public void testCreateIncreasingContainerWithLocalityRelaxation() {
    new ContainerRequest(Resource.newInstance(1024, 1),
        new String[] { "host1" }, null, Priority.newInstance(1), true,
        getContainerId(0));
  }
  
  @Test (expected = IllegalArgumentException.class) 
  public void testCreateIncreasingContainerWithMultipleHosts() {
    new ContainerRequest(Resource.newInstance(1024, 1),
        new String[] { "host1", "host2" }, null, Priority.newInstance(1), false,
        getContainerId(0));
  }
  
  @Test (expected = IllegalArgumentException.class) 
  public void testCreateIncreasingContainerWithRackSpecified() {
    new ContainerRequest(Resource.newInstance(1024, 1),
        new String[] { "host1" }, new String[] { "rack1" }, Priority.newInstance(1), false,
        getContainerId(0));
  }
  
  @Test 
  public void testCreateIncreasingContainer() {
    new ContainerRequest(Resource.newInstance(1024, 1),
        new String[] { "host1" }, null, Priority.newInstance(1), false,
        getContainerId(0));
  }
  
  private static class MyResolver implements DNSToSwitchMapping {

    @Override
    public List<String> resolve(List<String> names) {
      return Arrays.asList("/rack1");
    }

    @Override
    public void reloadCachedMappings() {}

    @Override
    public void reloadCachedMappings(List<String> names) {
    }
  }
  
  private ContainerId getContainerId(int id) {
    ContainerId containerId = ContainerId.newInstance(ApplicationAttemptId.newInstance(
        ApplicationId.newInstance(1234L, 1), 1), id);
    return containerId;
  }
  
  private void verifyResourceRequest(
      AMRMClientImpl<ContainerRequest> client, ContainerRequest request,
      String location, boolean expectedRelaxLocality) {
    verifyResourceRequest(client, request, location, 1, expectedRelaxLocality, null);
  }
  
  private void verifyResourceRequest(AMRMClientImpl<ContainerRequest> client,
      ContainerRequest request, String location, int numContainers,
      boolean expectedRelaxLocality, ContainerId existingContainerId) {
    ResourceRequest ask = client.remoteRequestsTable.get(request.getPriority())
        .get(location).get(request.getCapability()).remoteRequest;
    assertEquals(location, ask.getResourceName());
    assertEquals(numContainers, ask.getNumContainers());
    assertEquals(expectedRelaxLocality, ask.getRelaxLocality());
    if (existingContainerId != null) {
      Assert.assertNotNull(ask.getExistingContainerId());
      Assert.assertTrue(ask.getExistingContainerId().equals(existingContainerId));
    }
  }
}
