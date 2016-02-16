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
package edu.snu.mist.task.operators;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.formats.avro.InstantOperatorInfo;
import edu.snu.mist.formats.avro.InstantOperatorTypeEnum;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Maps and flattens the list of outputs.
 * @param <I> input type
 * @param <O> output type
 */
public final class FlatMapOperator<I, O> extends StatelessOperator<I, O> {
  private static final Logger LOG = Logger.getLogger(FlatMapOperator.class.getName());

  /**
   * FlatMap function.
   */
  private final MISTFunction<I, List<O>> flatMapFunc;

  @Inject
  private FlatMapOperator(@Parameter(QueryId.class) final String queryId,
                          @Parameter(OperatorId.class) final String operatorId,
                          final StringIdentifierFactory idfactory,
                          final MISTFunction<I, List<O>> flatMapFunc) {
    super(idfactory.getNewInstance(queryId), idfactory.getNewInstance(operatorId));
    this.flatMapFunc = flatMapFunc;
  }

  /**
   * FlatMaps the list of outputs.
   */
  @Override
  public void handle(final I input) {
    final List<O> outputs = flatMapFunc.apply(input);
    LOG.log(Level.FINE, "{0} FlatMaps {1} to {2}",
        new Object[]{FlatMapOperator.class, input, outputs});
    for (final O output : outputs) {
      outputEmitter.emit(output);
    }
  }


  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.FLAT_MAP;
  }

  @Override
  public SpecificRecord getAttribute() {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.FLAT_MAP);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(
        SerializationUtils.serialize(flatMapFunc)));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }
}
