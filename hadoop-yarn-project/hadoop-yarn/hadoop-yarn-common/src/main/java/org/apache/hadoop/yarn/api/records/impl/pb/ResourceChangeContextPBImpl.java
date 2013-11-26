package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceChangeContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceChangeContextProtoOrBuilder;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceProto;

public class ResourceChangeContextPBImpl extends ResourceChangeContext {
  ResourceChangeContextProto proto = ResourceChangeContextProto
      .getDefaultInstance();
  ResourceChangeContextProto.Builder builder = null;
  boolean viaProto = false;

  private ContainerId existingContainerId = null;
  private Resource targetCapability = null;

  public ResourceChangeContextPBImpl() {
    builder = ResourceChangeContextProto.newBuilder();
  }

  public ResourceChangeContextPBImpl(ResourceChangeContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceChangeContextProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public ContainerId getExistingContainerId() {
    ResourceChangeContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.existingContainerId != null) {
      return this.existingContainerId;
    }
    if (p.hasContainerId()) {
      this.existingContainerId = convertFromProtoFormat(p.getContainerId());
    }
    return this.existingContainerId;
  }

  @Override
  public void setExistingContainerId(ContainerId existingContainerId) {
    maybeInitBuilder();
    if (existingContainerId == null) {
      builder.clearContainerId();
    }
    this.existingContainerId = existingContainerId;
  }

  @Override
  public Resource getTargetCapability() {
    ResourceChangeContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.targetCapability != null) {
      return this.targetCapability;
    }
    if (p.hasTargetCapability()) {
      this.targetCapability = convertFromProtoFormat(p.getTargetCapability());
    }
    return this.targetCapability;
  }

  @Override
  public void setTargetCapability(Resource targetCapability) {
    maybeInitBuilder();
    if (targetCapability == null) {
      builder.clearTargetCapability();
    }
    this.targetCapability = targetCapability;
  }

  private ContainerIdPBImpl convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }

  private Resource convertFromProtoFormat(ResourceProto p) {
    return new ResourcePBImpl(p);
  }

  private ResourceProto convertToProtoFormat(Resource t) {
    return ((ResourcePBImpl) t).getProto();
  }

  private void mergeLocalToProto() {
    if (viaProto) {
      maybeInitBuilder();
    }
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = ResourceChangeContextProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.existingContainerId != null) {
      builder.setContainerId(convertToProtoFormat(this.existingContainerId));
    }
    if (this.targetCapability != null) {
      builder.setTargetCapability(convertToProtoFormat(this.targetCapability));
    }
  }
}
