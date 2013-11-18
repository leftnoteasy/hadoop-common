package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceResponse;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerIdPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceResponseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceResponseProtoOrBuilder;

import com.google.protobuf.TextFormat;

public class ChangeContainersResourceResponsePBImpl extends
    ChangeContainersResourceResponse {
  ChangeContainersResourceResponseProto proto = ChangeContainersResourceResponseProto
      .getDefaultInstance();
  ChangeContainersResourceResponseProto.Builder builder = null;
  boolean viaProto = false;

  private List<ContainerId> succeedChangedContainers = null;
  private List<ContainerId> failedChangedContainers = null;

  public ChangeContainersResourceResponsePBImpl() {
    builder = ChangeContainersResourceResponseProto.newBuilder();
  }

  public ChangeContainersResourceResponsePBImpl(
      ChangeContainersResourceResponseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public ChangeContainersResourceResponseProto getProto() {
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
    if (this.succeedChangedContainers != null) {
      addSucceedChangedContainersToProto();
    }
    if (this.failedChangedContainers != null) {
      addFailedChangedContainersToProto();
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
      builder = ChangeContainersResourceResponseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public List<ContainerId> getSucceedChangedContainers() {
    initSucceedChangedContainers();
    return this.succeedChangedContainers;
  }
  
  @Override
  public void setSucceedChangedContainers(List<ContainerId> containers) {
    if (containers == null) {
      return;
    }
    initSucceedChangedContainers();
    this.succeedChangedContainers.clear();
    this.succeedChangedContainers.addAll(containers);
  }

  private void initSucceedChangedContainers() {
    if (this.succeedChangedContainers != null) {
      return;
    }
    ChangeContainersResourceResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ContainerIdProto> list = p.getSucceedChangedContainersList();
    this.succeedChangedContainers = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.succeedChangedContainers.add(convertFromProtoFormat(c));
    }
  }

  private void addSucceedChangedContainersToProto() {
    maybeInitBuilder();
    builder.clearSucceedChangedContainers();
    if (this.succeedChangedContainers == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {
          Iterator<ContainerId> iter = succeedChangedContainers.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllSucceedChangedContainers(iterable);
  }

  @Override
  public List<ContainerId> getFailedChangedContainers() {
    initFailedChangedContainers();
    return this.failedChangedContainers;
  }

  @Override
  public void setFailedChangedContainers(List<ContainerId> containers) {
    if (containers == null) {
      return;
    }
    initFailedChangedContainers();
    this.failedChangedContainers.clear();
    this.failedChangedContainers.addAll(containers);
  }

  private void initFailedChangedContainers() {
    if (this.failedChangedContainers != null) {
      return;
    }
    ChangeContainersResourceResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ContainerIdProto> list = p.getFailedChangedContainersList();
    this.failedChangedContainers = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.failedChangedContainers.add(convertFromProtoFormat(c));
    }
  }

  private void addFailedChangedContainersToProto() {
    maybeInitBuilder();
    builder.clearFailedChangedContainers();
    if (this.failedChangedContainers == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {
          Iterator<ContainerId> iter = failedChangedContainers.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public ContainerIdProto next() {
            return convertToProtoFormat(iter.next());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
    builder.addAllFailedChangedContainers(iterable);
  }

  private ContainerId convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }
}
