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
import edu.snu.mist.api.functions.MISTPredicate;
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
 * Filter operator which filters input stream.
 * @param <I> input type
 */
public final class FilterOperator<I> extends StatelessOperator<I, I> {
  private static final Logger LOG = Logger.getLogger(FilterOperator.class.getName());

  /**
   * Filter function.
   */
  private final MISTPredicate<I> filterFunc;

  @Inject
  private FilterOperator(final MISTPredicate<I> filterFunc,
                         @Parameter(QueryId.class) final String queryId,
                         @Parameter(OperatorId.class) final String operatorId,
                         final StringIdentifierFactory idfactory) {
    super(idfactory.getNewInstance(queryId), idfactory.getNewInstance(operatorId));
    this.filterFunc = filterFunc;
  }


  /**
   * Filters the input.
   */
  @Override
  public void handle(final I input) {
    if (filterFunc.test(input)) {
      LOG.log(Level.FINE, "{0} Filters {1}",
          new Object[]{FilterOperator.class, input});
      outputEmitter.emit(input);
    }
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.FILTER;
  }

  @Override
  public SpecificRecord getAttribute() {
    final InstantOperatorInfo.Builder iOpInfoBuilder = InstantOperatorInfo.newBuilder();
    iOpInfoBuilder.setInstantOperatorType(InstantOperatorTypeEnum.FILTER);
    final List<ByteBuffer> serializedFunctionList = new ArrayList<>();
    serializedFunctionList.add(ByteBuffer.wrap(
        SerializationUtils.serialize(filterFunc)));
    iOpInfoBuilder.setFunctions(serializedFunctionList);
    iOpInfoBuilder.setKeyIndex(null);
    return iOpInfoBuilder.build();
  }
}