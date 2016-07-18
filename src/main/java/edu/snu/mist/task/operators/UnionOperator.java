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
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.operators.parameters.OperatorId;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Union operator which unifies some continuous stream.
 * @param <I> input type
 */
public final class UnionOperator<I> extends StatelessOperator<I, I> {
  private static final Logger LOG = Logger.getLogger(UnionOperator.class.getName());

  @Inject
  private UnionOperator(@Parameter(QueryId.class) final String queryId,
                        @Parameter(OperatorId.class) final String operatorId,
                        final StringIdentifierFactory idfactory) {
    super(idfactory.getNewInstance(queryId), idfactory.getNewInstance(operatorId));
  }


  /**
   * Unifies the input.
   * Fortunately, union is already done in logical plan generating level through connecting DAG.
   */
  @Override
  public void handle(final I input) {
    LOG.log(Level.FINE, "{0} Filters {1}",
        new Object[]{UnionOperator.class, input});
    outputEmitter.emit(input);
  }

  @Override
  public StreamType.OperatorType getOperatorType() {
    return StreamType.OperatorType.UNION;
  }
}