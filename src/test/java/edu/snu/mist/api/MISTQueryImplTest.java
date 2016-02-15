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
import edu.snu.mist.api.sources.REEFNetworkSourceStream;
import edu.snu.mist.api.types.Tuple2;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * The test class for SourceConfiguration.
 */
public final class MISTQueryImplTest {

  /**
   * Test for MISTQueryImpl.
   */
  @Test
  public void testMISTQuery() {
    final Sink sink = new REEFNetworkSourceStream<String>(APITestParameters.LOCAL_REEF_NETWORK_SOURCE_CONF)
        .flatMap(s -> Arrays.asList(s.split(" ")))
        .map(s -> new Tuple2<>(s, 1))
        .reduceByKey(0, String.class, (Integer x, Integer y) -> x + y)
        .reefNetworkOutput(APITestParameters.LOCAL_REEF_NETWORK_SINK_CONF);
    final MISTQuery query = sink.getQuery();

    Assert.assertTrue(query.getQuerySinks().contains(sink));

    final Set<Sink> sinkSet = new HashSet<>(Arrays.asList(sink));
    final MISTQuery sinkSetQuery = new MISTQueryImpl(sinkSet);
    Assert.assertEquals(sinkSetQuery.getQuerySinks(), sinkSet);
  }
}