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
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTSupplier;
import edu.snu.mist.common.DAG;
import edu.snu.mist.formats.avro.InstantOperatorInfo;
import edu.snu.mist.formats.avro.InstantOperatorTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class implements the necessary methods for getting information
 * about user-defined aggregation operators on windowed stream.
 * This is different to ApplyStatefulOperatorStream in that
 * it it receives a Collection of input data, and
 * it maintains no internal state inside but just apply the stateful functions to the collected data from the window.
 */
public final class AggregateWindowOperatorStream<IN, OUT, S>
    extends InstantOperatorStream<Collection<IN>, OUT> {

  /**
   * BiFunction used for updating the temporal state.
   */
  private final MISTBiFunction<IN, S, S> updateStateFunc;
  /**
   * Function used for producing the result stream from temporal state.
   */
  private final MISTFunction<S, OUT> produceResultFunc;
  /**
   * Supplier used for initializing state.
   */
  private final MISTSupplier<S> initializeStateSup;

  public AggregateWindowOperatorStream(final MISTBiFunction<IN, S, S> updateStateFunc,
                                       final MISTFunction<S, OUT> produceResultFunc,
                                       final MISTSupplier<S> initializeStateSup,
                                       final DAG<AvroVertexSerializable, StreamType.Direction> dag) {
    super(StreamType.OperatorType.AGGREGATE_WINDOW, dag);
    this.updateStateFunc = updateStateFunc;
    this.produceResultFunc = produceResultFunc;
    this.initializeStateSup = initializeStateSup;
  }

  /**
   * @return the Function with two arguments used for updating its internal state
   */
  public MISTBiFunction<IN, S, S> getUpdateStateFunc() {
    return updateStateFunc;
  }

  /**
   * @return the Function with one argument used for producing results
   */
  public MISTFunction<S, OUT> getProduceResultFunc() {
    return produceResultFunc;
  }

  /**
   * @return the supplier generating the state of operation.
   */
  public MISTSupplier<S> getInitializeStateSup() {
    return initializeStateSup;
  }

  @Override
  protected InstantOperatorInfo getInstantOpInfo() {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.AGGREGATE_WINDOW);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(updateStateFunc)));
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(produceResultFunc)));
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(initializeStateSup)));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }
}