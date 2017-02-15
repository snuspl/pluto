/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.common.operators;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistWatermarkEvent;
import edu.snu.mist.common.parameters.OperatorId;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Conditional branch operator which branches out from input stream.
 * TODO: [MIST-417] Implement control flow in task side
 * @param <I> input type
 */
public final class ConditionalBranchOperator<I> extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(ConditionalBranchOperator.class.getName());

  @Inject
  private ConditionalBranchOperator(
      @Parameter(OperatorId.class) final String operatorId,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    this(operatorId);
  }

  @Inject
  public ConditionalBranchOperator(@Parameter(OperatorId.class) final String operatorId) {
    super(operatorId);
  }

  /**
   * Checks the branch conditions and forward to matched branch.
   */
  @Override
  public void processLeftData(final MistDataEvent input) {
    final I value = (I)input.getValue();
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}