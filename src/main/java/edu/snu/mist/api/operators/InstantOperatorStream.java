/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.api.operators;

import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.ContinuousStreamImpl;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.InstantOperatorInfo;
import edu.snu.mist.formats.avro.Vertex;
import edu.snu.mist.formats.avro.VertexTypeEnum;

/**
 * A ContinuousStream created by operation on any kind of MISTStream.
 */
abstract class InstantOperatorStream<IN, OUT> extends ContinuousStreamImpl<OUT> {
  /**
   * The type of this operator (e.g. filterOperator, mapOperator, ...)
   */
  private final StreamType.OperatorType operatorType;

  protected InstantOperatorStream(final StreamType.OperatorType operatorType,
                                  final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(StreamType.ContinuousType.OPERATOR, dag);
    this.operatorType = operatorType;
  }

  /**
   * @return The type of the current operator (ex: filter, map, flatMap, ...)
   */
  public StreamType.OperatorType getOperatorType() {
    return operatorType;
  }

  /**
   * Get an instant operator info.
   */
  protected abstract InstantOperatorInfo getInstantOpInfo();

  @Override
  public Vertex getSerializedVertex() {
    final Vertex.Builder vertexBuilder = Vertex.newBuilder();
    vertexBuilder.setVertexType(VertexTypeEnum.INSTANT_OPERATOR);
    vertexBuilder.setAttributes(getInstantOpInfo());
    return vertexBuilder.build();
  }
}