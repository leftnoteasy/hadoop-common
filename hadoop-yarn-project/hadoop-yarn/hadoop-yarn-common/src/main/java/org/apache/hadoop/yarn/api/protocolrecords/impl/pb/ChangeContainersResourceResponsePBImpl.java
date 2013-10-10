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

  private List<ContainerId> succeedIncreasedContainers = null;
  private List<ContainerId> succeedDecreasedContainers = null;
  private List<ContainerId> failedIncreasedContainers = null;
  private List<ContainerId> failedDecreasedContainers = null;

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
    if (this.succeedIncreasedContainers != null) {
      addSucceedIncreasedContainersToProto();
    }
    if (this.succeedDecreasedContainers != null) {
      addSucceedDecreasedContainersToProto();
    }
    if (this.failedIncreasedContainers != null) {
      addFailedIncreasedContainersToProto();
    }
    if (this.failedDecreasedContainers != null) {
      addFailedDecreasedContainersToProto();
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
  public List<ContainerId> getSucceedIncreasedContainers() {
    initSucceedIncreasedContainers();
    return this.succeedIncreasedContainers;
  }

  @Override
  public void setSucceedIncreasedContainers(List<ContainerId> containers) {
    if (containers == null) {
      return;
    }
    initSucceedIncreasedContainers();
    this.succeedIncreasedContainers.clear();
    this.succeedIncreasedContainers.addAll(containers);
  }

  private void initSucceedIncreasedContainers() {
    if (this.succeedIncreasedContainers != null) {
      return;
    }
    ChangeContainersResourceResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ContainerIdProto> list = p.getSucceedIncreasedContainersList();
    this.succeedIncreasedContainers = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.succeedIncreasedContainers.add(convertFromProtoFormat(c));
    }
  }

  private void addSucceedIncreasedContainersToProto() {
    maybeInitBuilder();
    builder.clearSucceedIncreasedContainers();
    if (this.succeedIncreasedContainers == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {
          Iterator<ContainerId> iter = succeedIncreasedContainers.iterator();

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
    builder.addAllSucceedIncreasedContainers(iterable);
  }

  @Override
  public List<ContainerId> getSucceedDecreasedContainers() {
    initSucceedDecreasedContainers();
    return this.succeedDecreasedContainers;
  }

  @Override
  public void setSucceedDecreasedContainers(List<ContainerId> containers) {
    if (containers == null) {
      return;
    }
    initSucceedDecreasedContainers();
    this.succeedDecreasedContainers.clear();
    this.succeedDecreasedContainers.addAll(containers);
  }

  private void initSucceedDecreasedContainers() {
    if (this.succeedDecreasedContainers != null) {
      return;
    }
    ChangeContainersResourceResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ContainerIdProto> list = p.getSucceedDecreasedContainersList();
    this.succeedDecreasedContainers = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.succeedDecreasedContainers.add(convertFromProtoFormat(c));
    }
  }

  private void addSucceedDecreasedContainersToProto() {
    maybeInitBuilder();
    builder.clearSucceedDecreasedContainers();
    if (this.succeedDecreasedContainers == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {
          Iterator<ContainerId> iter = succeedDecreasedContainers.iterator();

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
    builder.addAllSucceedDecreasedContainers(iterable);
  }

  @Override
  public List<ContainerId> getFailedIncreasedContainers() {
    initFailedIncreasedContainers();
    return this.failedIncreasedContainers;
  }

  @Override
  public void setFailedIncreasedContainers(List<ContainerId> containers) {
    if (containers == null) {
      return;
    }
    initFailedIncreasedContainers();
    this.failedIncreasedContainers.clear();
    this.failedIncreasedContainers.addAll(containers);
  }

  private void initFailedIncreasedContainers() {
    if (this.failedIncreasedContainers != null) {
      return;
    }
    ChangeContainersResourceResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ContainerIdProto> list = p.getFailedIncreasedContainersList();
    this.failedIncreasedContainers = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.failedIncreasedContainers.add(convertFromProtoFormat(c));
    }
  }

  private void addFailedIncreasedContainersToProto() {
    maybeInitBuilder();
    builder.clearFailedIncreasedContainers();
    if (this.failedIncreasedContainers == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {
          Iterator<ContainerId> iter = failedIncreasedContainers.iterator();

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
    builder.addAllFailedIncreasedContainers(iterable);
  }

  @Override
  public List<ContainerId> getFailedDecreasedContainers() {
    initFailedDecreasedContainers();
    return this.failedDecreasedContainers;
  }

  @Override
  public void setFailedDecreasedContainers(List<ContainerId> containers) {
    if (containers == null) {
      return;
    }
    initFailedDecreasedContainers();
    this.failedDecreasedContainers.clear();
    this.failedDecreasedContainers.addAll(containers);
  }

  private void initFailedDecreasedContainers() {
    if (this.failedDecreasedContainers != null) {
      return;
    }
    ChangeContainersResourceResponseProtoOrBuilder p = viaProto ? proto
        : builder;
    List<ContainerIdProto> list = p.getFailedDecreasedContainersList();
    this.failedDecreasedContainers = new ArrayList<ContainerId>();

    for (ContainerIdProto c : list) {
      this.failedDecreasedContainers.add(convertFromProtoFormat(c));
    }
  }

  private void addFailedDecreasedContainersToProto() {
    maybeInitBuilder();
    builder.clearFailedDecreasedContainers();
    if (this.failedDecreasedContainers == null) {
      return;
    }
    Iterable<ContainerIdProto> iterable = new Iterable<ContainerIdProto>() {
      @Override
      public Iterator<ContainerIdProto> iterator() {
        return new Iterator<ContainerIdProto>() {
          Iterator<ContainerId> iter = failedDecreasedContainers.iterator();

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
    builder.addAllFailedDecreasedContainers(iterable);
  }

  private ContainerId convertFromProtoFormat(ContainerIdProto p) {
    return new ContainerIdPBImpl(p);
  }

  private ContainerIdProto convertToProtoFormat(ContainerId t) {
    return ((ContainerIdPBImpl) t).getProto();
  }
}
