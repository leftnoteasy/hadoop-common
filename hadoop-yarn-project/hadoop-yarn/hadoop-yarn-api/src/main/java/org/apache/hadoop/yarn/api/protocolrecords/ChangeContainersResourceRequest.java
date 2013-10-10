package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.util.Records;

/**
 * <p>
 * The request sent by <code>ApplicationMaster</code> to ask
 * <code>NodeManager</code> change resource quota used by a specified container
 * </p>
 * 
 * @see ContainerManagementProtocol#changeContainersResource(ChangeContainersResourceRequest)
 */
@Public
public abstract class ChangeContainersResourceRequest {
	@Public
	public static ChangeContainersResourceRequest newInstance(
	    List<ResourceIncreaseContext> containersToIncrease,
	    List<ResourceChangeContext> containersToDecrease) {
		ChangeContainersResourceRequest request = Records
		    .newRecord(ChangeContainersResourceRequest.class);
		request.setContainersToIncrease(containersToIncrease);
		request.setContainersToDecrease(containersToDecrease);
		return request;
	}

	@Public
	public abstract List<ResourceIncreaseContext> getContainersToIncrease();

	@Public
	public abstract void setContainersToIncrease(
	    List<ResourceIncreaseContext> containersToIncrease);

	@Public
	public abstract List<ResourceChangeContext> getContainersToDecrease();

	@Public
	public abstract void setContainersToDecrease(
	    List<ResourceChangeContext> containersToDecrease);
}
