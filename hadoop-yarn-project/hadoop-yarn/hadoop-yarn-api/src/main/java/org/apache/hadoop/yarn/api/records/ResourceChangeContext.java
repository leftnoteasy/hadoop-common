package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.util.Records;

@Public
public abstract class ResourceChangeContext {
  @Public
  public static ResourceChangeContext newInstance(
      ContainerId existingContainerId, Resource targetCapability) {
    ResourceChangeContext context = Records
        .newRecord(ResourceChangeContext.class);
    context.setExistingContainerId(existingContainerId);
    context.setTargetCapability(targetCapability);
    return context;
  }

  @Public
  public abstract ContainerId getExistingContainerId();

  @Public
  public abstract void setExistingContainerId(ContainerId existingContainerId);

  @Public
  public abstract Resource getTargetCapability();

  @Public
  public abstract void setTargetCapability(Resource targetCapability);

  @Override
  public boolean equals(Object other) {
    if (other instanceof ResourceChangeContext) {
      ResourceChangeContext ctx = (ResourceChangeContext)other;
      
      if ((getExistingContainerId() == null) ^ (ctx.getExistingContainerId() == null)) {
        return false;
      } else if (ctx.getExistingContainerId() != null) {
        if (!getExistingContainerId().equals(ctx.getExistingContainerId())) {
          return false;
        }
      }
      
      if ((getTargetCapability() == null) ^ (ctx.getTargetCapability() == null)) {
        return false;
      } else if (ctx.getTargetCapability() != null) {
        if (!getTargetCapability().equals(ctx.getTargetCapability())) {
          return false;
        }
      }
      
      return true;
    } else {
      return false;
    }
  }
}
