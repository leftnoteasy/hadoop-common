package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.util.Records;

@Public
public abstract class ChangeContainersResourceResponse {
	@Public
	public static ChangeContainersResourceResponse newInstance(
	    List<ContainerId> succeedIncreasedContainers,
	    List<ContainerId> succeedDecreasedContainers,
	    List<ContainerId> failedIncreasedContainers,
	    List<ContainerId> failedDecreasedContainers) {
		ChangeContainersResourceResponse request = Records
		    .newRecord(ChangeContainersResourceResponse.class);
		request.setSucceedIncreasedContainers(succeedIncreasedContainers);
		request.setSucceedDecreasedContainers(succeedDecreasedContainers);
		request.setFailedIncreasedContainers(failedIncreasedContainers);
		request.setFailedDecreasedContainers(failedDecreasedContainers);
		return request;
	}

	@Public
	public abstract List<ContainerId> getSucceedIncreasedContainers();

	@Public
	public abstract void setSucceedIncreasedContainers(
	    List<ContainerId> succeedIncreasedContainers);

	@Public
	public abstract List<ContainerId> getSucceedDecreasedContainers();

	@Public
	public abstract void setSucceedDecreasedContainers(
	    List<ContainerId> succeedDecreasedContainers);

	@Public
	public abstract List<ContainerId> getFailedIncreasedContainers();

	@Public
	public abstract void setFailedIncreasedContainers(
	    List<ContainerId> failedIncreasedContainers);

	@Public
	public abstract List<ContainerId> getFailedDecreasedContainers();

	@Public
	public abstract void setFailedDecreasedContainers(
	    List<ContainerId> failedDecreasedContainers);

}
