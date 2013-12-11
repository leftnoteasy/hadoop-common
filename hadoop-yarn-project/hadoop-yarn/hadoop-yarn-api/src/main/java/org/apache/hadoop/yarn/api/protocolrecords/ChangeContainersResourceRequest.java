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

package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The request sent by <code>Application Master</code> to ask
 * <code>Node Manager</code> change resource quota used by a specified container
 * </p>
 * 
 * @see ContainerManagementProtocol#changeContainersResource(ChangeContainersResourceRequest)
 */
@Public
@Stable
public abstract class ChangeContainersResourceRequest {
  @Public
  @Stable
  public static ChangeContainersResourceRequest newInstance(
      List<Token> containersTokensToIncrease,
      List<ContainerResourceDecrease> containersToDecrease) {
    ChangeContainersResourceRequest request =
        Records.newRecord(ChangeContainersResourceRequest.class);
    request.setContainersToIncrease(containersTokensToIncrease);
    request.setContainersToDecrease(containersToDecrease);
    return request;
  }

  /**
   * get tokens of container need to be increased.
   */
  @Public
  @Stable
  public abstract List<Token> getContainersToIncrease();

  /** 
   * set tokens of containers need to be increased, token is acquired in
   * <code>AllocateResponse.getIncreasedContainers</code>. Here we only need
   * the token in <code>ContainerResourceIncrease</code> because container id
   * and capability are already contained in token
   */
  @Public
  @Stable
  public abstract void setContainersToIncrease(
      List<Token> containersTokensToIncrease);

  /**
   * get decrease containers sent by <code>ApplicationMaster</code>
   */
  @Public
  @Stable
  public abstract List<ContainerResourceDecrease> getContainersToDecrease();

  /**
   * set containers need to be decreased by <code>ApplicationMaster</code> to
   * <code>NodeManager</code>
   */
  @Public
  @Stable
  public abstract void setContainersToDecrease(
      List<ContainerResourceDecrease> containersToDecrease);
}
