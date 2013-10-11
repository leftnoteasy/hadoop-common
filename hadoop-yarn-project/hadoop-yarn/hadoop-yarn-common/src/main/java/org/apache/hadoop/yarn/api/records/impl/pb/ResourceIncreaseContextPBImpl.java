package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceChangeContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceIncreaseContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceIncreaseContextProtoOrBuilder;

public class ResourceIncreaseContextPBImpl extends ResourceIncreaseContext {
  ResourceIncreaseContextProto proto = ResourceIncreaseContextProto
      .getDefaultInstance();
  ResourceIncreaseContextProto.Builder builder = null;
  boolean viaProto = false;

  private ResourceChangeContext context = null;
  private Token token = null;

  public ResourceIncreaseContextPBImpl() {
    builder = ResourceIncreaseContextProto.newBuilder();
    viaProto = true;
  }

  public ResourceIncreaseContextPBImpl(ResourceIncreaseContextProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ResourceIncreaseContextProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public ResourceChangeContext getResourceChangeContext() {
    ResourceIncreaseContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.context != null) {
      return this.context;
    }
    if (p.hasContext()) {
      this.context = convertFromProtoFormat(p.getContext());
    }
    return context;
  }

  @Override
  public void setResourceChangeContext(ResourceChangeContext context) {
    maybeInitBuilder();
    if (context == null) {
      builder.clearContext();
    }
    this.context = context;
  }

  @Override
  public Token getContainerToken() {
    ResourceIncreaseContextProtoOrBuilder p = viaProto ? proto : builder;
    if (this.token != null) {
      return this.token;
    }
    if (p.hasContainerToken()) {
      this.token = convertFromProtoFormat(p.getContainerToken());
    }
    return token;
  }

  @Override
  public void setContainerToken(Token token) {
    maybeInitBuilder();
    if (token == null) {
      builder.clearContainerToken();
    }
    this.token = token;
  }

  private ResourceChangeContext convertFromProtoFormat(
      ResourceChangeContextProto p) {
    return new ResourceChangeContextPBImpl(p);
  }

  private ResourceChangeContextProto convertToProtoFormat(
      ResourceChangeContext t) {
    return ((ResourceChangeContextPBImpl) t).getProto();
  }

  private Token convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
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
      builder = ResourceIncreaseContextProto.newBuilder(proto);
    }
    viaProto = false;
  }

  private void mergeLocalToBuilder() {
    if (this.context != null) {
      builder.setContext(convertToProtoFormat(this.context));
    }
    if (this.token != null) {
      builder.setContainerToken(convertToProtoFormat(this.token));
    }
  }
}
