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

package edu.snu.mist.api;

import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sink.SinkImpl;
import edu.snu.mist.api.sink.builder.DefaultSinkConfigurationImpl;
import edu.snu.mist.api.sink.builder.SinkConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * The test class for SourceConfiguration.
 */
public class MISTQueryImplTest {

  /**
   * Test for MISTQueryImpl.
   */
  @Test
  public void testMISTQuery() {
    final SinkConfiguration sinkConfiguration = new DefaultSinkConfigurationImpl(new HashMap<>());
    final Sink sink = new SinkImpl(new MISTStreamImpl(StreamType.BasicType.CONTINUOUS),
        StreamType.SinkType.REEF_NETWORK_SINK, sinkConfiguration);
    final MISTQuery singleSinkQuery = new MISTQueryImpl(sink);

    Assert.assertTrue(singleSinkQuery.getQuerySinks().contains(sink));

    final Set<Sink> sinkSet = new HashSet<>(Arrays.asList(sink));
    final MISTQuery sinkSetQuery = new MISTQueryImpl(sinkSet);
    Assert.assertEquals(sinkSetQuery.getQuerySinks(), sinkSet);
  }
}
