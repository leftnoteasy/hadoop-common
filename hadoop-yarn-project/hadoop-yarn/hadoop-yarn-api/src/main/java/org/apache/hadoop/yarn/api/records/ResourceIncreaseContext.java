package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.yarn.util.Records;

@Public
public abstract class ResourceIncreaseContext {
  @Public
  public static ResourceIncreaseContext newInstance(
      ResourceChangeContext changeContext, Token containerToken) {
    ResourceIncreaseContext context = Records
        .newRecord(ResourceIncreaseContext.class);
    context.setResourceChangeContext(changeContext);
    context.setContainerToken(containerToken);
    return context;
  }

  @Public
  public abstract ResourceChangeContext getResourceChangeContext();

  @Public
  public abstract void setResourceChangeContext(ResourceChangeContext context);

  @Public
  public abstract Token getContainerToken();

  @Public
  public abstract void setContainerToken(Token token);
  
  @Override
  public boolean equals(Object other) {
    if (other instanceof ResourceIncreaseContext) {
      ResourceIncreaseContext ctx = (ResourceIncreaseContext)other;
      if ((getResourceChangeContext() == null) ^ (ctx == null)) {
        return false;
      } else {
        if (getResourceChangeContext() != null) {
          return getResourceChangeContext().equals(ctx.getResourceChangeContext());
        }
      }
      return true;
    } else {
      return false;
    }
  }
}
