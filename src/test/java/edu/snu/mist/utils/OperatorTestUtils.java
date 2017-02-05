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
package edu.snu.mist.utils;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.functions.*;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.WindowData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.reef.io.Tuple;
import org.junit.Assert;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This is a utility class for operator test.
 */
public final class OperatorTestUtils {

  private OperatorTestUtils() {
    // empty constructor
  }

  /**
   * Checks the windowed result is equal to the expected result.
   */
  public static void checkWindowData(final MistEvent result,
                                     final Collection<Integer> expectedResult,
                                     final long expectedWindowStartMoment,
                                     final long expectedWindowSize,
                                     final long expectedWindowTimestamp) {
    Assert.assertTrue(result.isData());
    Assert.assertTrue(((MistDataEvent)result).getValue() instanceof WindowData);
    final WindowData windowData = (WindowData)((MistDataEvent)result).getValue();
    Assert.assertEquals(expectedResult, windowData.getDataCollection());
    Assert.assertEquals(expectedWindowStartMoment, windowData.getStart());
    Assert.assertEquals(expectedWindowSize, windowData.getEnd() - windowData.getStart() + 1);
    Assert.assertEquals(expectedWindowTimestamp, result.getTimestamp());
  }

  /**
   * A test function for binding class.
   */
  public static final class TestFunction implements MISTFunction<String, List<String>> {
    @Inject
    private TestFunction() {

    }
    @Override
    public List<String> apply(final String s) {
      return new ArrayList<>();
    }
  }

  /**
   * A test predicate for binding class.
   */
  public static final class TestPredicate implements MISTPredicate<String> {
    @Inject
    private TestPredicate() {

    }
    @Override
    public boolean test(final String s) {
      return true;
    }
  }

  /**
   * A test biFunction for binding class.
   */
  public static final class TestBiFunction implements MISTBiFunction<Integer, Integer, Integer> {
    @Inject
    private TestBiFunction() {

    }
    @Override
    public Integer apply(final Integer s, final Integer s2) {
      return s + s2;
    }
  }


  /**
   * A test biPredicate for binding class.
   */
  public static final class TestBiPredicate implements MISTBiPredicate<String, String> {
    @Inject
    private TestBiPredicate() {

    }
    @Override
    public boolean test(final String s, final String s2) {
      return true;
    }
  }

  /**
   * A simple ApplyStatefulFunction that counts String which starts with capital A.
   */
  public static final class TestApplyStatefulFunction
      implements ApplyStatefulFunction<Tuple2<String, Integer>, Integer> {
    // the internal state
    private int state;

    @Inject
    public TestApplyStatefulFunction() {
    }

    @Override
    public void initialize() {
      this.state = 0;
    }

    @Override
    public void update(final Tuple2<String, Integer> input) {
      if (((String)input.get(0)).startsWith("A")) {
        state++;
      }
    }

    @Override
    public Integer getCurrentState() {
      return state;
    }

    @Override
    public Integer produceResult() {
      return state;
    }
  }

  public static final class TestNettyTimestampExtractFunc implements MISTFunction<String, Tuple<String, Long>> {
    @Inject
    private TestNettyTimestampExtractFunc() {

    }
    @Override
    public Tuple<String, Long> apply(final String s) {
      return new Tuple<>(s, 1L);
    }
  }

  public static final class TestKafkaTimestampExtractFunc
      implements MISTFunction<ConsumerRecord, Tuple<ConsumerRecord, Long>> {
    @Inject
    private TestKafkaTimestampExtractFunc() {

    }
    @Override
    public Tuple<ConsumerRecord, Long> apply(final ConsumerRecord consumerRecord) {
      return new Tuple<>(consumerRecord, 1L);
    }
  }

  public static final class TestWatermarkTimestampExtractFunc implements WatermarkTimestampFunction<String> {
    @Inject
    private TestWatermarkTimestampExtractFunc() {

    }
    @Override
    public Long apply(final String s) {
      return 1L;
    }
  }
}
