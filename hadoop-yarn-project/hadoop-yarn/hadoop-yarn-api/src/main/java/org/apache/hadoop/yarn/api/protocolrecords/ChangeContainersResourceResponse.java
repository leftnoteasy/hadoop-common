package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Records;

@Public
public abstract class ChangeContainersResourceResponse {
  @Public
  public static ChangeContainersResourceResponse newInstance(
      List<ContainerId> succeedChangedContainers,
      List<ContainerId> failedChangedContainers) {
    ChangeContainersResourceResponse request = Records
        .newRecord(ChangeContainersResourceResponse.class);
    request.setSucceedChangedContainers(succeedChangedContainers);
    request.setFailedChangedContainers(failedChangedContainers);
    return request;
  }

  @Public
  public abstract List<ContainerId> getSucceedChangedContainers();

  @Public
  public abstract void setSucceedChangedContainers(
      List<ContainerId> succeedIncreasedContainers);
  
  @Public
  public abstract List<ContainerId> getFailedChangedContainers();

  @Public
  public abstract void setFailedChangedContainers(
      List<ContainerId> succeedIncreasedContainers);
}
