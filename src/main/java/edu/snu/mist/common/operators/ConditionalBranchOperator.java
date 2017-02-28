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
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.parameters.SerializedUdfList;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Conditional branch operator which branches out from input stream.
 * @param <I> input type
 */
public final class ConditionalBranchOperator<I> extends OneStreamOperator {
  private static final Logger LOG = Logger.getLogger(ConditionalBranchOperator.class.getName());

  private final List<MISTPredicate<I>> predicates;

  @Inject
  private ConditionalBranchOperator(
      @Parameter(SerializedUdfList.class) final List<String> serializedUdfList,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    predicates = new ArrayList<>(serializedUdfList.size());
    for (final String serializedUdf : serializedUdfList) {
      predicates.add(SerializeUtils.deserializeFromString(serializedUdf, classLoader));
    }
  }

  @Inject
  public ConditionalBranchOperator(final List<MISTPredicate<I>> predicates) {
    this.predicates = predicates;
  }

  /**
   * Checks the branch conditions and forward to matched branch.
   */
  @Override
  public void processLeftData(final MistDataEvent input) {
    final I value = (I)input.getValue();
    for (int i = 0; i < predicates.size(); i++) {
      // test the input data with each predicate
      final MISTPredicate<I> predicate = predicates.get(i);
      if (predicate.test(value)) {
        // if there is matched predicate, emit the data with the index of matched predicate
        outputEmitter.emitData(input, i + 1);
        return;
      }
    }
  }

  @Override
  public void processLeftWatermark(final MistWatermarkEvent input) {
    outputEmitter.emitWatermark(input);
  }
}