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
package edu.snu.mist.api.serialize.avro;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.operators.*;
import edu.snu.mist.formats.avro.InstantOperatorInfo;
import edu.snu.mist.formats.avro.InstantOperatorTypeEnum;
import org.apache.commons.lang.SerializationUtils;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Default implementation class for InstantOperatorInfoProvider.
 */
public final class InstantOperatorInfoProviderImpl implements InstantOperatorInfoProvider {

  @Inject
  private InstantOperatorInfoProviderImpl() {

  }

  @Override
  public InstantOperatorInfo getInstantOperatorInfo(final InstantOperatorStream iOpStream) {
    if (iOpStream.getOperatorType() == StreamType.OperatorType.APPLY_STATEFUL) {
      return getApplyStatefulOpInfo((ApplyStatefulOperatorStream) iOpStream);
    } else if (iOpStream.getOperatorType() == StreamType.OperatorType.FILTER) {
      return getFilterOpInfo((FilterOperatorStream) iOpStream);
    } else if (iOpStream.getOperatorType() == StreamType.OperatorType.FLAT_MAP) {
      return getFlatMapOpInfo((FlatMapOperatorStream) iOpStream);
    } else if (iOpStream.getOperatorType() == StreamType.OperatorType.MAP) {
      return getMapOpInfo((MapOperatorStream) iOpStream);
    } else if (iOpStream.getOperatorType() == StreamType.OperatorType.REDUCE_BY_KEY) {
      return getReduceByKeyOpInfo((ReduceByKeyOperatorStream) iOpStream);
    } else if (iOpStream.getOperatorType() == StreamType.OperatorType.REDUCE_BY_KEY_WINDOW) {
      return getReduceByKeyWindowInfo((ReduceByKeyWindowOperatorStream) iOpStream);
    } else {
      throw new IllegalStateException("Illegal InstantOperatorStream type!");
    }
  }

  private InstantOperatorInfo getApplyStatefulOpInfo(final ApplyStatefulOperatorStream applyStatefulOperatorStream) {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.APPLY_STATEFUL);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(
        applyStatefulOperatorStream.getUpdateStateFunc())));
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(
        applyStatefulOperatorStream.getProduceResultFunc())));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }

  private InstantOperatorInfo getFilterOpInfo(final FilterOperatorStream filterOperatorStream) {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.FILTER);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(
        SerializationUtils.serialize(filterOperatorStream.getFilterFunction())));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }

  private InstantOperatorInfo getFlatMapOpInfo(final FlatMapOperatorStream flatMapOperatorStream) {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.FLAT_MAP);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(
        SerializationUtils.serialize(flatMapOperatorStream.getFlatMapFunction())));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }

  private InstantOperatorInfo getMapOpInfo(final MapOperatorStream mapOperatorStream) {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.MAP);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(
        SerializationUtils.serialize(mapOperatorStream.getMapFunction())));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }

  private InstantOperatorInfo getReduceByKeyOpInfo(final ReduceByKeyOperatorStream reduceByKeyOperatorStream) {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.REDUCE_BY_KEY);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(
        SerializationUtils.serialize(reduceByKeyOperatorStream.getReduceFunction())));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(reduceByKeyOperatorStream.getKeyFieldIndex());
    return iOpInfoBuilder.build();
  }

  private InstantOperatorInfo getReduceByKeyWindowInfo(
      final ReduceByKeyWindowOperatorStream reduceByKeyWindowOperatorStream) {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.REDUCE_BY_KEY_WINDOW);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(SerializationUtils.serialize(
        reduceByKeyWindowOperatorStream.getReduceFunction())));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(reduceByKeyWindowOperatorStream.getKeyFieldIndex());
    return iOpInfoBuilder.build();
  }
}