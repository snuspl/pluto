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

import edu.snu.mist.api.APITestParameters;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.WindowedStream;
import edu.snu.mist.api.sources.REEFNetworkSourceStream;
import edu.snu.mist.api.types.Tuple2;
import edu.snu.mist.api.window.TimeEmitPolicy;
import edu.snu.mist.api.window.TimeSizePolicy;
import org.junit.Assert;
import org.junit.Test;

/**
 * The test class for WindowedStream and operations on WindowedStream.
 */
public class WindowedStreamTest {

  private final WindowedStream<Tuple2<String, Integer>> windowedStream
      = new REEFNetworkSourceStream<String>(APITestParameters.TEST_REEF_NETWORK_SOURCE_CONF)
      .map(s -> new Tuple2<>(s, 1))
      /* Creates a test windowed stream with 5 sec size and emits windowed stream every 1 sec */
      .window(new TimeSizePolicy(5000), new TimeEmitPolicy(1000));

  /**
   * Test for creating WindowedStream from ContinuousStream.
   */
  @Test
  public void testWindowedStream() {
    Assert.assertEquals(windowedStream.getBasicType(), StreamType.BasicType.WINDOWED);
    Assert.assertEquals(windowedStream.getWindowSizePolicy(), new TimeSizePolicy(5000));
    Assert.assertEquals(windowedStream.getWindowEmitPolicy(), new TimeEmitPolicy(1000));
  }

  /**
   * Test for reduceByKeyWindow operation.
   */
  @Test
  public void testReduceByKeyWindowStream() {
    final ReduceByKeyWindowOperatorStream<Tuple2<String, Integer>, String, Integer> reducedWindowStream
        = windowedStream.reduceByKeyWindow(0, String.class, (x, y) -> x + y);
    Assert.assertEquals(reducedWindowStream.getBasicType(), StreamType.BasicType.CONTINUOUS);
    Assert.assertEquals(reducedWindowStream.getContinuousType(), StreamType.ContinuousType.OPERATOR);
    Assert.assertEquals(reducedWindowStream.getOperatorType(), StreamType.OperatorType.REDUCE_BY_KEY_WINDOW);
    Assert.assertEquals(reducedWindowStream.getKeyFieldIndex(), 0);
    Assert.assertEquals(reducedWindowStream.getReduceFunction().apply(1, 2), (Integer)3);
    Assert.assertNotEquals(reducedWindowStream.getReduceFunction().apply(1, 3), (Integer)3);
    Assert.assertEquals(reducedWindowStream.getInputStreams().iterator()
        .next().getBasicType(), StreamType.BasicType.WINDOWED);
  }
}