/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.api.protocolrecords.impl.pb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.proto.SecurityProtos.TokenProto;
import org.apache.hadoop.yarn.api.protocolrecords.ChangeContainersResourceRequest;
import org.apache.hadoop.yarn.api.records.ContainerResourceDecrease;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerResourceDecreasePBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.TokenPBImpl;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerResourceDecreaseProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceRequestProto;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.ChangeContainersResourceRequestProtoOrBuilder;

import com.google.protobuf.TextFormat;

@Private
@Unstable
public class ChangeContainersResourceRequestPBImpl extends
    ChangeContainersResourceRequest {
  ChangeContainersResourceRequestProto proto =
      ChangeContainersResourceRequestProto.getDefaultInstance();
  ChangeContainersResourceRequestProto.Builder builder = null;
  boolean viaProto = false;

  private List<Token> containersToIncrease = null;
  private List<ContainerResourceDecrease> containersToDecrease = null;

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
  public List<Token> getContainersToIncrease() {
    initContainersToIncrease();
    return this.containersToIncrease;
  }

  @Override
  public void setContainersToIncrease(List<Token> containersToIncrease) {
    if (containersToIncrease == null) {
      return;
    }
    initContainersToIncrease();
    this.containersToIncrease.clear();
    this.containersToIncrease.addAll(containersToIncrease);
  }

  @Override
  public List<ContainerResourceDecrease> getContainersToDecrease() {
    initContainersToDecrease();
    return this.containersToDecrease;
  }

  @Override
  public void setContainersToDecrease(
      List<ContainerResourceDecrease> containersToDecrease) {
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
    ChangeContainersResourceRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<TokenProto> list = p.getIncreaseContainersList();
    this.containersToIncrease = new ArrayList<Token>();

    for (TokenProto c : list) {
      this.containersToIncrease.add(convertFromProtoFormat(c));
    }
  }

  private void addIncreaseContainersToProto() {
    maybeInitBuilder();
    builder.clearIncreaseContainers();
    if (this.containersToIncrease == null) {
      return;
    }
    Iterable<TokenProto> iterable = new Iterable<TokenProto>() {
      @Override
      public Iterator<TokenProto> iterator() {
        return new Iterator<TokenProto>() {
          Iterator<Token> iter = containersToIncrease.iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public TokenProto next() {
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
    ChangeContainersResourceRequestProtoOrBuilder p =
        viaProto ? proto : builder;
    List<ContainerResourceDecreaseProto> list = p.getDecreaseContainersList();
    this.containersToDecrease = new ArrayList<ContainerResourceDecrease>();

    for (ContainerResourceDecreaseProto c : list) {
      this.containersToDecrease.add(convertFromProtoFormat(c));
    }
  }

  private void addDecreaseContainersToProto() {
    maybeInitBuilder();
    builder.clearDecreaseContainers();
    if (this.containersToDecrease == null) {
      return;
    }
    Iterable<ContainerResourceDecreaseProto> iterable =
        new Iterable<ContainerResourceDecreaseProto>() {
          @Override
          public Iterator<ContainerResourceDecreaseProto> iterator() {
            return new Iterator<ContainerResourceDecreaseProto>() {
              Iterator<ContainerResourceDecrease> iter = containersToDecrease
                  .iterator();

              @Override
              public boolean hasNext() {
                return iter.hasNext();
              }

              @Override
              public ContainerResourceDecreaseProto next() {
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

  private Token convertFromProtoFormat(TokenProto p) {
    return new TokenPBImpl(p);
  }

  private TokenProto convertToProtoFormat(Token t) {
    return ((TokenPBImpl) t).getProto();
  }

  private ContainerResourceDecrease convertFromProtoFormat(
      ContainerResourceDecreaseProto p) {
    return new ContainerResourceDecreasePBImpl(p);
  }

  private ContainerResourceDecreaseProto convertToProtoFormat(
      ContainerResourceDecrease t) {
    return ((ContainerResourceDecreasePBImpl) t).getProto();
  }
}
