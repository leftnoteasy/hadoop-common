package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.util.Records;

@Public
public abstract class ResourceChangeContext {
	@Public
	public static ResourceChangeContext newInstance(ContainerId existingContainerId,
	    Resource targetCapability) {
		ResourceChangeContext context = Records
		    .newRecord(ResourceChangeContext.class);
		context.setExistingContainerId(existingContainerId);
		context.setTargetCapability(targetCapability);
		return context;
	}

	@Public
	public abstract ContainerId getExistingContainerId(
	    ContainerId existingContainerId);

	@Public
	public abstract void setExistingContainerId(ContainerId existingContainerId);

	@Public
	public abstract Resource getTargetCapability();

	@Public
	public abstract void setTargetCapability(Resource targetCapability);
}
