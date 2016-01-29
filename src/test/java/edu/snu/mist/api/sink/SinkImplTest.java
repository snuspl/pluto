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
package edu.snu.mist.api.sink;

import edu.snu.mist.api.MISTStream;
import edu.snu.mist.api.MISTStreamImpl;
import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.builder.DefaultSinkConfigurationImpl;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * The test class for SinkConfiguration.
 */
public class SinkImplTest {
  /**
   * Test for SinkImpl.
   */
  @Test
  public void testSinkImpl() {
    final SinkConfiguration sinkConfiguration = new DefaultSinkConfigurationImpl(new HashMap<>());
    final MISTStream mistStream = new MISTStreamImpl(StreamType.BasicType.CONTINUOUS);
    final Sink sink = new SinkImpl(mistStream, StreamType.SinkType.REEF_NETWORK_SINK, sinkConfiguration);

    Assert.assertEquals(sink.getSinkType(), StreamType.SinkType.REEF_NETWORK_SINK);
    Assert.assertSame(sink.getSinkConfiguration(), sinkConfiguration);
    Assert.assertTrue(sink.getPrecedingStreams().contains(mistStream));
    Assert.assertTrue(sink.getQuery().getQuerySinks().contains(sink));
  }
}
