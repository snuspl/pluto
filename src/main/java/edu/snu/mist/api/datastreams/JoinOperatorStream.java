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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.AvroVertexSerializable;
import edu.snu.mist.api.functions.MISTBiPredicate;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.formats.avro.WindowOperatorInfo;
import edu.snu.mist.formats.avro.WindowOperatorTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class describes a join operator stream which gets windowed and unified stream, and
 * joins a pair of inputs in two streams that satisfies the user-defined predicate maintaining the window.
 * @param <T> the type of first input DataStream
 * @param <U> the type of second input DataStream
 */
public final class JoinOperatorStream<T, U> extends WindowedStreamImpl<Tuple2<T, U>> {

  /**
   * The user-defined predicate which checks whether two inputs from both stream are matched or not.
   */
  private final MISTBiPredicate<T, U> joinBiPredicate;

  public JoinOperatorStream(final MISTBiPredicate<T, U> joinBiPredicate,
                            final DAG<AvroVertexSerializable, Direction> dag) {
    super(dag);
    this.joinBiPredicate = joinBiPredicate;
  }

  /**
   * @return The user-defined predicate to check whether two inputs from both stream are matched or not.
   */
  public MISTBiPredicate<T, U> getJoinBiPredicate() {
    return joinBiPredicate;
  }

  @Override
  protected WindowOperatorInfo getWindowOpInfo() {
    final WindowOperatorInfo.Builder wOpInfoBuilder = WindowOperatorInfo.newBuilder();
    wOpInfoBuilder.setWindowOperatorType(WindowOperatorTypeEnum.JOIN);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(joinBiPredicate)));
    wOpInfoBuilder.setFunctions(serializedFunctionList);
    wOpInfoBuilder.setWindowSize(0);
    wOpInfoBuilder.setWindowInterval(0);
    return wOpInfoBuilder.build();
  }
}