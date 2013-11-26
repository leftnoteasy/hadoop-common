package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceChangeContext;
import org.apache.hadoop.yarn.api.records.ResourceIncreaseContext;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceChangeContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourceIncreaseContextPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceChangeContextProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ResourceIncreaseContextProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
public class ChangeContainersResourceRequestPBImpl extends
    ChangeContainersResourceRequest {
  ChangeContainersResourceRequestProto proto = ChangeContainersResourceRequestProto
      .getDefaultInstance();
  ChangeContainersResourceRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<ResourceIncreaseContext> containersToIncrease = null;
  private List<ResourceChangeContext> containersToDecrease = null;

  public ChangeContainersResourceRequestPBImpl() {
    builder = ChangeContainersResourceRequestProto.newBuilder();
  }

  public ChangeContainersResourceRequestPBImpl(
      ChangeContainersResourceRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ChangeContainersResourceRequestProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {
    if (this.containersToIncrease != null) {
      addIncreaseContainersToProto();
    }
    if (this.containersToDecrease != null) {
      addDecreaseContainersToProto();
    }
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
      builder = ChangeContainersResourceRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<ResourceIncreaseContext> getContainersToIncrease() {
    initContainersToIncrease();
    return this.containersToIncrease;
  }

  @Override
  public void setContainersToIncrease(
      List<ResourceIncreaseContext> containersToIncrease) {
    if (containersToIncrease == null) {
      return;
    }
    initContainersToIncrease();
    this.containersToIncrease.clear();
    this.containersToIncrease.addAll(containersToIncrease);
  }

  @Override
  public List<ResourceChangeContext> getContainersToDecrease() {
    initContainersToDecrease();
    return this.containersToDecrease;
  }

  @Override
  public void setContainersToDecrease(
      List<ResourceChangeContext> containersToDecrease) {
    if (containersToDecrease == null) {
      return;
    }
    initContainersToDecrease();
    this.containersToDecrease.clear();
    this.containersToDecrease.addAll(containersToDecrease);
  }

  private void initContainersToIncrease() {
    if (this.containersToIncrease != null) {
      return;
    }
    ChangeContainersResourceRequestProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ResourceIncreaseContextProto> list = p.getIncreaseContainersList();
    this.containersToIncrease = new ArrayList<ResourceIncreaseContext>();

    for (ResourceIncreaseContextProto c : list) {
      this.containersToIncrease.add(convertFromProtoFormat(c));
    }
  }

  private void addIncreaseContainersToProto() {
    maybeInitBuilder();
    builder.clearIncreaseContainers();
    if (this.containersToIncrease == null) {
      return;
    }
    Iterable<ResourceIncreaseContextProto> iterable = new Iterable<ResourceIncreaseContextProto>() {
      @Override
      public Iterator<ResourceIncreaseContextProto> iterator() {
        return new Iterator<ResourceIncreaseContextProto>() {
          Iterator<ResourceIncreaseContext> iter = containersToIncrease
              .iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceIncreaseContextProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllIncreaseContainers(iterable);
  }

  private void initContainersToDecrease() {
    if (this.containersToDecrease != null) {
      return;
    }
    ChangeContainersResourceRequestProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ResourceChangeContextProto> list = p.getDecreaseContainersList();
    this.containersToDecrease = new ArrayList<ResourceChangeContext>();

    for (ResourceChangeContextProto c : list) {
      this.containersToDecrease.add(convertFromProtoFormat(c));
    }
  }

  private void addDecreaseContainersToProto() {
    maybeInitBuilder();
    builder.clearDecreaseContainers();
    if (this.containersToDecrease == null) {
      return;
    }
    Iterable<ResourceChangeContextProto> iterable = new Iterable<ResourceChangeContextProto>() {
      @Override
      public Iterator<ResourceChangeContextProto> iterator() {
        return new Iterator<ResourceChangeContextProto>() {
          Iterator<ResourceChangeContext> iter = containersToDecrease
              .iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ResourceChangeContextProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllDecreaseContainers(iterable);
  }

  private ResourceChangeContext convertFromProtoFormat(
      ResourceChangeContextProto p) {
    return new ResourceChangeContextPBImpl(p);
  }

  private ResourceChangeContextProto convertToProtoFormat(
      ResourceChangeContext t) {
    return ((ResourceChangeContextPBImpl) t).getProto();
  }

  private ResourceIncreaseContext convertFromProtoFormat(
      ResourceIncreaseContextProto p) {
    return new ResourceIncreaseContextPBImpl(p);
  }

  private ResourceIncreaseContextProto convertToProtoFormat(
      ResourceIncreaseContext t) {
    return ((ResourceIncreaseContextPBImpl) t).getProto();
  }
}
