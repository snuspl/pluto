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
package edu.snu.mist.api.datastreams;

import edu.snu.mist.api.MISTQueryBuilder;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.SerializeUtils;
import edu.snu.mist.common.functions.ApplyStatefulFunction;
import edu.snu.mist.common.functions.MISTBiFunction;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.parameters.KeyIndex;
import edu.snu.mist.common.parameters.SerializedUdf;
import edu.snu.mist.common.types.Tuple2;
import edu.snu.mist.common.windows.TimeWindowInformation;
import edu.snu.mist.common.windows.WindowData;
import edu.snu.mist.formats.avro.Direction;
import edu.snu.mist.utils.OperatorTestUtils;
import edu.snu.mist.utils.TestParameters;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * The test class for WindowedStream and operations on WindowedStream.
 */
public class WindowedStreamTest {

  private MISTQueryBuilder queryBuilder;
  private WindowedStream<Tuple2<String, Integer>> timeWindowedStream;

  @Before
  public void setUp() {
    queryBuilder = new MISTQueryBuilder();
    timeWindowedStream = queryBuilder.socketTextStream(TestParameters.LOCAL_TEXT_SOCKET_SOURCE_CONF)
        .map(s -> new Tuple2<>(s, 1))
        .window(new TimeWindowInformation(5000, 1000));
  }

  @After
  public void tearDown() {
    queryBuilder = null;
  }


  /**
   * Test for reduceByKeyWindow operation.
   */
  @Test
  public void testReduceByKeyWindowStream() throws InjectionException, IOException {
    final MISTBiFunction<Integer, Integer, Integer> reduceFunc = (x, y) -> x + y;
    final ContinuousStream<Map<String, Integer>> reducedWindowStream =
        timeWindowedStream.reduceByKeyWindow(0, String.class, reduceFunc);

    // Get info
    final Injector injector = Tang.Factory.getTang().newInjector(reducedWindowStream.getConfiguration());
    final int desKeyIndex = injector.getNamedInstance(KeyIndex.class);
    final String serializedFunc = injector.getNamedInstance(SerializedUdf.class);
    Assert.assertEquals(0, desKeyIndex);
    Assert.assertEquals(SerializeUtils.serializeToString(reduceFunc), serializedFunc);

    // Check windowed -> reduce by key
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream,
        reducedWindowStream, new Tuple<>(Direction.LEFT, 0));
  }

  /**
   * Test for binding the udf class of reduceByKeyWindow operation.
   */
  @Test
  public void testReduceByKeyWindowClassBinding() throws InjectionException {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final ContinuousStream<Map<String, Integer>> reducedWindowStream =
        timeWindowedStream.reduceByKeyWindow(0, String.class, OperatorTestUtils.TestBiFunction.class, funcConf);

    // Get info
    final Injector injector = Tang.Factory.getTang().newInjector(reducedWindowStream.getConfiguration());
    final int desKeyIndex = injector.getNamedInstance(KeyIndex.class);
    final MISTBiFunction biFunction = injector.getInstance(MISTBiFunction.class);
    Assert.assertEquals(0, desKeyIndex);
    Assert.assertTrue(biFunction instanceof OperatorTestUtils.TestBiFunction);

    // Check windowed -> reduce by key
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream,
        reducedWindowStream, new Tuple<>(Direction.LEFT, 0));
  }

  /**
   * Test for binding the udf class of applyStatefulWindow operation.
   */
  @Test
  public void testApplyStatefulWindowStream() throws InjectionException, IOException {
    final ApplyStatefulFunction<Tuple2<String, Integer>, Integer> func =
        new OperatorTestUtils.TestApplyStatefulFunction();
    final ContinuousStream<Integer> applyStatefulWindowStream =
        timeWindowedStream.applyStatefulWindow(new OperatorTestUtils.TestApplyStatefulFunction());

    /* Simulate two data inputs on UDF stream */
    final Configuration conf = applyStatefulWindowStream.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final String seFunc = injector.getNamedInstance(SerializedUdf.class);
    Assert.assertEquals(SerializeUtils.serializeToString(func), seFunc);

    // Check windowed -> stateful operation applied
    checkEdges(
        queryBuilder.build().getDAG(), 1, timeWindowedStream,
        applyStatefulWindowStream, new Tuple<>(Direction.LEFT, 0));
  }

  /**
   * Test for applyStatefulWindow operation.
   */
  @Test
  public void testApplyStatefulWindowClassBinding() throws InjectionException {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final ContinuousStream<Integer> applyStatefulWindowStream =
        timeWindowedStream.applyStatefulWindow(OperatorTestUtils.TestApplyStatefulFunction.class, funcConf);

    /* Simulate two data inputs on UDF stream */
    final Configuration conf = applyStatefulWindowStream.getConfiguration();
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    final ApplyStatefulFunction func = injector.getInstance(ApplyStatefulFunction.class);
    Assert.assertTrue(func instanceof OperatorTestUtils.TestApplyStatefulFunction);

    // Check windowed -> stateful operation applied
    checkEdges(
        queryBuilder.build().getDAG(), 1, timeWindowedStream,
        applyStatefulWindowStream, new Tuple<>(Direction.LEFT, 0));
  }

  /**
   * Test for aggregateWindow operation.
   */
  @Test
  public void testAggregateWindowStream() throws InjectionException, IOException, ClassNotFoundException {
    final MISTFunction<WindowData<Tuple2<String, Integer>>, String> func = new WindowAggregateFunction();
    final ContinuousStream<String> aggregateWindowStream
        = timeWindowedStream.aggregateWindow(func);

    final Injector injector = Tang.Factory.getTang().newInjector(aggregateWindowStream.getConfiguration());
    final String serializedFunc = injector.getNamedInstance(SerializedUdf.class);
    Assert.assertEquals(SerializeUtils.serializeToString(func), serializedFunc);
    // Check windowed -> aggregated
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream,
        aggregateWindowStream, new Tuple<>(Direction.LEFT, 0));
  }

  /**
   * Test for binding the udf class of aggregateWindow operation.
   */
  @Test
  public void testAggregateWindowClassBinding() throws InjectionException {
    final Configuration funcConf = Tang.Factory.getTang().newConfigurationBuilder().build();
    final ContinuousStream<String> aggregateWindowStream
        = timeWindowedStream.aggregateWindow(WindowAggregateFunction.class, funcConf);

    final Injector injector = Tang.Factory.getTang().newInjector(aggregateWindowStream.getConfiguration());
    final MISTFunction func = injector.getInstance(MISTFunction.class);
    Assert.assertTrue(func instanceof WindowAggregateFunction);
    // Check windowed -> aggregated
    checkEdges(queryBuilder.build().getDAG(), 1, timeWindowedStream,
        aggregateWindowStream, new Tuple<>(Direction.LEFT, 0));
  }

  static final class WindowAggregateFunction implements MISTFunction<WindowData<Tuple2<String, Integer>>, String> {
    @Inject
    public WindowAggregateFunction() {

    }

    @Override
    public String apply(final WindowData<Tuple2<String, Integer>> windowData) {
      String result = "";
      final Iterator<Tuple2<String, Integer>> itr = windowData.getDataCollection().iterator();
      while(itr.hasNext()) {
        final Tuple2<String, Integer> tuple = itr.next();
        result = result.concat("{" + tuple.get(0) + ", " + tuple.get(1).toString() + "}, ");
      }
      return result + windowData.getStart() + ", " + windowData.getEnd();
    }
  }

  /**
   * Checks the size and direction of the edges from upstream.
   */
  private void checkEdges(final DAG<MISTStream, Tuple<Direction, Integer>> dag,
                          final int edgesSize,
                          final MISTStream upStream,
                          final MISTStream downStream,
                          final Tuple<Direction, Integer> edgeInfo) {
    final Map<MISTStream, Tuple<Direction, Integer>> neighbors = dag.getEdges(upStream);
    Assert.assertEquals(edgesSize, neighbors.size());
    Assert.assertEquals(edgeInfo, neighbors.get(downStream));
  }
}