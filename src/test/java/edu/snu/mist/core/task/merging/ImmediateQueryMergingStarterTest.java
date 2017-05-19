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
package edu.snu.mist.core.task.merging;

import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.OutputEmitter;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.graph.DAG;
import edu.snu.mist.common.graph.GraphUtils;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.common.operators.FilterOperator;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.core.task.*;
import edu.snu.mist.core.task.utils.IdAndConfGenerator;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.*;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test whether ImmediateQueryMergingStarter merges and starts the submitted queries correctly.
 */
public final class ImmediateQueryMergingStarterTest {

  private IdAndConfGenerator idAndConfGenerator;
  private ExecutionVertexGenerator executionVertexGenerator;
  private ConfigExecutionVertexMap configExecutionVertexMap;
  private ExecutionVertexCountMap executionVertexCountMap;
  private ExecutionVertexDagMap executionVertexDagMap;
  private ExecutionDags executionDags;
  private QueryIdConfigDagMap queryIdConfigDagMap;
  private SrcAndDagMap<String> srcAndDagMap;
  private QueryStarter queryStarter;

  @Before
  public void setUp() throws InjectionException, IOException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    jcb.bindImplementation(QueryStarter.class, ImmediateQueryMergingStarter.class);
    idAndConfGenerator = new IdAndConfGenerator();
    executionVertexGenerator = mock(ExecutionVertexGenerator.class);

    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    injector.bindVolatileInstance(ExecutionVertexGenerator.class, executionVertexGenerator);

    configExecutionVertexMap = injector.getInstance(ConfigExecutionVertexMap.class);
    executionVertexCountMap = injector.getInstance(ExecutionVertexCountMap.class);
    executionVertexDagMap = injector.getInstance(ExecutionVertexDagMap.class);
    executionDags = injector.getInstance(ExecutionDags.class);
    queryIdConfigDagMap = injector.getInstance(QueryIdConfigDagMap.class);
    srcAndDagMap = injector.getInstance(SrcAndDagMap.class);
    queryStarter = injector.getInstance(QueryStarter.class);
  }

  /**
   * Get a test source with the configuration.
   * @param conf source configuration
   * @return test source
   */
  private TestSource generateSource(final String conf) {
    return new TestSource(idAndConfGenerator.generateId(), conf);
  }

  /**
   * Get a simple operator chain that has a filter operator.
   * @param conf configuration of the operator
   * @return operator chain
   */
  private OperatorChain generateFilterOperatorChain(final String conf,
                                                    final MISTPredicate<String> predicate) {
    final OperatorChain operatorChain = new DefaultOperatorChainImpl("testOpChain");
    final PhysicalOperator filterOp = new DefaultPhysicalOperatorImpl(idAndConfGenerator.generateId(),
        conf, new FilterOperator<>(predicate), operatorChain);
    operatorChain.insertToHead(filterOp);
    return operatorChain;
  }

  /**
   * Get a sink that stores the outputs to the list.
   * @param conf configuration of the sink
   * @param result list for storing outputs
   * @return sink
   */
  private PhysicalSink<String> generateSink(final String conf,
                                            final List<String> result) {
    return new PhysicalSinkImpl<>(idAndConfGenerator.generateId(), conf, new TestSink<>(result));
  }

  /**
   * Generate a simple query that has the following structure: src -> operator chain -> sink.
   * @param source source
   * @param operatorChain operator chain
   * @param sink sink
   * @return dag
   */
  private Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>> generateSimpleDag(
      final TestSource source,
      final OperatorChain operatorChain,
      final PhysicalSink<String> sink,
      final ConfigVertex srcVertex,
      final ConfigVertex ocVertex,
      final ConfigVertex sinkVertex) throws IOException, InjectionException {
    // Create DAG
    final DAG<ConfigVertex, MISTEdge> dag = new AdjacentListConcurrentMapDAG<>();
    dag.addVertex(srcVertex);
    dag.addVertex(ocVertex);
    dag.addVertex(sinkVertex);

    dag.addEdge(srcVertex, ocVertex, new MISTEdge(Direction.LEFT));
    dag.addEdge(ocVertex, sinkVertex, new MISTEdge(Direction.LEFT));

    final DAG<ExecutionVertex, MISTEdge> executionDag = new AdjacentListConcurrentMapDAG<>();
    executionDag.addVertex(source);
    executionDag.addVertex(operatorChain);
    executionDag.addVertex(sink);

    executionDag.addEdge(source, operatorChain, new MISTEdge(Direction.LEFT));
    executionDag.addEdge(operatorChain, sink, new MISTEdge(Direction.LEFT));

    when(executionVertexGenerator.generate(eq(srcVertex), any(URL[].class), any(ClassLoader.class)))
        .thenReturn(source);
    when(executionVertexGenerator.generate(eq(ocVertex), any(URL[].class), any(ClassLoader.class)))
        .thenReturn(operatorChain);
    when(executionVertexGenerator.generate(eq(sinkVertex), any(URL[].class), any(ClassLoader.class)))
        .thenReturn(sink);

    return new Tuple<>(dag, executionDag);
  }

  /**
   * Test cases
   * Case 1. Start a single query
   * Case 2. Two queries have one source but the sources are different.
   * Case 3. Two queries have one same source, and same operator chain
   * Case 4. Two queries have one same, source, but different operator chain
   * Case 5. Two queries have two sources, same two sources, same operator chains
   * Case 6. Two queries have two sources, one same source, one different source
   * Case 7. Three queries - two execution Dags and one submitted Dag
   *  - The submitted query has two same sources with the two execution dags
   */

  /**
   * Case 1: Start a single query.
   * Test if it executes a single query correctly when there are no execution dags that are currently running
   */
  @Test
  public void singleQueryMergingTest() throws InjectionException, IOException, ClassNotFoundException {
    final List<String> result = new LinkedList<>();

   // Physical vertices
    final String sourceConf = idAndConfGenerator.generateConf();
    final String ocConf = idAndConfGenerator.generateConf();
    final String sinkConf = idAndConfGenerator.generateConf();
    final TestSource source = generateSource(sourceConf);
    final OperatorChain operatorChain = generateFilterOperatorChain(ocConf, (s) -> true);
    final PhysicalSink<String> sink = generateSink(sinkConf, result);

    // Config vertices
    final ConfigVertex srcVertex = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf));
    final ConfigVertex ocVertex = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(ocConf));
    final ConfigVertex sinkVertex = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple = generateSimpleDag(source, operatorChain, sink,
        srcVertex, ocVertex, sinkVertex);

    // Execute the query 1
    final List<String> paths = mock(List.class);
    queryStarter.start("q1", dagTuple.getKey(), paths);

    // Generate events for the query and check if the dag is executed correctly
    final String data1 = "Hello";
    source.send(data1);
    Assert.assertEquals(Arrays.asList(), result);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    Assert.assertEquals(true, operatorChain.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result);

    // Check queryIdConfigDagMap
    Assert.assertEquals(dagTuple.getKey(), queryIdConfigDagMap.get("q1"));

    // Check srcAndDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple.getValue(), srcAndDagMap.get(sourceConf)));

    // Check executionVertexDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple.getValue(), executionVertexDagMap.get(source)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple.getValue(), executionVertexDagMap.get(operatorChain)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple.getValue(), executionVertexDagMap.get(sink)));

    // Check configExecutionVertexMap
    Assert.assertEquals(source, configExecutionVertexMap.get(srcVertex));
    Assert.assertEquals(operatorChain, configExecutionVertexMap.get(ocVertex));
    Assert.assertEquals(sink, configExecutionVertexMap.get(sinkVertex));

    // Check reference count
    Assert.assertEquals(1, (int)executionVertexCountMap.get(source));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(operatorChain));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink));

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(srcAndDagMap.get(sourceConf));
    Assert.assertEquals(expectedDags, executionDags.values());
  }

  /**
   * Case 2: Merging two queries that have different sources.
   */
  @Test
  public void mergingDifferentSourceQueriesOneGroupTest()
      throws InjectionException, IOException, ClassNotFoundException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final List<String> paths1 = mock(List.class);

    // Physical vertices
    final String sourceConf1 = idAndConfGenerator.generateConf();
    final String ocConf1 = idAndConfGenerator.generateConf();
    final String sinkConf1 = idAndConfGenerator.generateConf();
    final TestSource src1 = generateSource(sourceConf1);
    final OperatorChain operatorChain1 = generateFilterOperatorChain(ocConf1, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(sinkConf1, result1);

    // Config vertices
    final ConfigVertex srcVertex1 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf1));
    final ConfigVertex ocVertex1 = new ConfigVertex(ExecutionVertex.Type.OPERATOR_CHAIN, Arrays.asList(ocConf1));
    final ConfigVertex sinkVertex1 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf1));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple1 = generateSimpleDag(src1, operatorChain1, sink1,
        srcVertex1, ocVertex1, sinkVertex1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 is different from that of src1.
    final List<String> result2 = new LinkedList<>();
    final List<String> paths2 = mock(List.class);

    // Physical vertices
    final String sourceConf2 = idAndConfGenerator.generateConf();
    final String ocConf2 = idAndConfGenerator.generateConf();
    final String sinkConf2 = idAndConfGenerator.generateConf();
    final TestSource src2 = generateSource(sourceConf2);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(ocConf2, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(sinkConf2, result2);

    // Config vertices
    final ConfigVertex srcVertex2 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf2));
    final ConfigVertex ocVertex2 = new ConfigVertex(ExecutionVertex.Type.OPERATOR_CHAIN, Arrays.asList(ocConf2));
    final ConfigVertex sinkVertex2 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf2));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple2 = generateSimpleDag(src2, operatorChain2, sink2,
        srcVertex2, ocVertex2, sinkVertex2);

    // Execute two queries
    final String query1Id = "q1";
    final String query2Id = "q2";
    queryStarter.start(query1Id, dagTuple1.getKey(), paths1);
    queryStarter.start(query2Id, dagTuple2.getKey(), paths2);

    // The query 1 and 2 have different sources, so they should be executed separately
    final String data1 = "Hello";
    src1.send(data1);
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
    Assert.assertEquals(false, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(), result2);

    final String data2 = "World";
    src2.send(data2);
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data2), result2);
    Assert.assertEquals(false, operatorChain1.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);

    // Check queryIdConfigDagMap
    Assert.assertEquals(dagTuple1.getKey(), queryIdConfigDagMap.get(query1Id));
    Assert.assertEquals(dagTuple2.getKey(), queryIdConfigDagMap.get(query2Id));

    // Check srcAndDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), srcAndDagMap.get(sourceConf1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple2.getValue(), srcAndDagMap.get(sourceConf2)));

    // Check executionVertexDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(src1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(operatorChain1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(sink1)));

    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple2.getValue(), executionVertexDagMap.get(src2)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple2.getValue(), executionVertexDagMap.get(operatorChain2)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple2.getValue(), executionVertexDagMap.get(sink2)));

    // Check configExecutionVertexMap
    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex1));
    Assert.assertEquals(operatorChain1, configExecutionVertexMap.get(ocVertex1));
    Assert.assertEquals(sink1, configExecutionVertexMap.get(sinkVertex1));

    Assert.assertEquals(src2, configExecutionVertexMap.get(srcVertex2));
    Assert.assertEquals(operatorChain2, configExecutionVertexMap.get(ocVertex2));
    Assert.assertEquals(sink2, configExecutionVertexMap.get(sinkVertex2));

    // Check reference count
    Assert.assertEquals(1, (int)executionVertexCountMap.get(src1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(operatorChain1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(src2));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(operatorChain2));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink2));

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(srcAndDagMap.get(sourceConf1));
    expectedDags.add(srcAndDagMap.get(sourceConf2));

    Assert.assertEquals(expectedDags, executionDags.values());
  }

  /**
   * Case 3: Merging two dags that have same source and operator chain.
   * @throws InjectionException
   */
  @Test
  public void mergingSameSourceAndSameOperatorQueriesOneGroupTest()
      throws InjectionException, IOException, ClassNotFoundException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final String sourceConf = idAndConfGenerator.generateConf();
    final String operatorConf = idAndConfGenerator.generateConf();
    final TestSource src1 = generateSource(sourceConf);
    final String sinkConf1 = idAndConfGenerator.generateConf();
    final OperatorChain operatorChain1 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(sinkConf1, result1);

    // Config vertices
    final ConfigVertex srcVertex1 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf));
    final ConfigVertex ocVertex1 = new ConfigVertex(ExecutionVertex.Type.OPERATOR_CHAIN, Arrays.asList(operatorConf));
    final ConfigVertex sinkVertex1 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf1));
    final List<String> paths1 = mock(List.class);

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple1 = generateSimpleDag(src1, operatorChain1, sink1,
        srcVertex1, ocVertex1, sinkVertex1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 and operatorChain2 is same as that of src1 and operatorChain2.
    final List<String> result2 = new LinkedList<>();
    final String sinkConf2 = idAndConfGenerator.generateConf();
    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(sinkConf2, result2);
    final List<String> paths2 = mock(List.class);

    // Config vertices
    final ConfigVertex srcVertex2 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf));
    final ConfigVertex ocVertex2 = new ConfigVertex(ExecutionVertex.Type.OPERATOR_CHAIN, Arrays.asList(operatorConf));
    final ConfigVertex sinkVertex2 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf2));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple2 = generateSimpleDag(src2, operatorChain2, sink2,
        srcVertex2, ocVertex2, sinkVertex2);

    // Execute two queries
    final String query1Id = "q1";
    final String query2Id = "q2";
    queryStarter.start(query1Id, dagTuple1.getKey(), paths1);
    queryStarter.start(query2Id, dagTuple2.getKey(), paths2);

    // Generate events for the merged query and check if the dag is executed correctly
    final String data = "Hello";
    src1.send(data);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(0, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(false, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data), result1);
    Assert.assertEquals(Arrays.asList(data), result2);

    // Src2 and 1 are the same, so the output emitter of src2 should be null
    try {
      src2.send(data);
      Assert.fail("OutputEmitter should be null");
    } catch (final NullPointerException e) {
      // do nothing
    }

    // Check queryIdConfigDagMap
    Assert.assertEquals(dagTuple1.getKey(), queryIdConfigDagMap.get(query1Id));
    Assert.assertEquals(dagTuple2.getKey(), queryIdConfigDagMap.get(query2Id));

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    final DAG<ExecutionVertex, MISTEdge> mergedDag = dagTuple1.getValue();
    mergedDag.addVertex(sink2);
    mergedDag.addEdge(operatorChain1, sink2, new MISTEdge(Direction.LEFT));
    expectedDags.add(srcAndDagMap.get(sourceConf));
    Assert.assertEquals(expectedDags, executionDags.values());

    // Check srcAndDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, srcAndDagMap.get(sourceConf)));

    // Check executionVertexDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(src1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(operatorChain1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(sink1)));

    // They are merged, so src2, oc2 and sink2 should be included in dag1
    Assert.assertNull(executionVertexDagMap.get(src2));
    Assert.assertNull(executionVertexDagMap.get(operatorChain2));
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(sink2)));

    // Check configExecutionVertexMap
    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex1));
    Assert.assertEquals(operatorChain1, configExecutionVertexMap.get(ocVertex1));
    Assert.assertEquals(sink1, configExecutionVertexMap.get(sinkVertex1));

    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex2));
    Assert.assertEquals(operatorChain1, configExecutionVertexMap.get(ocVertex2));
    Assert.assertEquals(sink2, configExecutionVertexMap.get(sinkVertex2));

    // Check reference count
    Assert.assertEquals(2, (int)executionVertexCountMap.get(src1));
    Assert.assertEquals(2, (int)executionVertexCountMap.get(operatorChain1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink1));
    Assert.assertNull(executionVertexCountMap.get(src2));
    Assert.assertNull(executionVertexCountMap.get(operatorChain2));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink2));

  }

  /**
   * Case 4: Merging two dags that have same source but different operator chain.
   * @throws InjectionException
   */
  @Test
  public void mergingSameSourceButDifferentOperatorQueriesOneGroupTest()
      throws InjectionException, IOException, ClassNotFoundException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final String sourceConf = idAndConfGenerator.generateConf();
    final String ocConf1 = idAndConfGenerator.generateConf();
    final String sinkConf1 = idAndConfGenerator.generateConf();
    final List<String> paths1 = mock(List.class);

    final TestSource src1 = generateSource(sourceConf);
    final OperatorChain operatorChain1 = generateFilterOperatorChain(ocConf1, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(sinkConf1, result1);

    // Config vertices
    final ConfigVertex srcVertex1 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf));
    final ConfigVertex ocVertex1 = new ConfigVertex(ExecutionVertex.Type.OPERATOR_CHAIN, Arrays.asList(ocConf1));
    final ConfigVertex sinkVertex1 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf1));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple1 = generateSimpleDag(src1, operatorChain1, sink1,
        srcVertex1, ocVertex1, sinkVertex1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 and operatorChain2 is same as that of src1 and operatorChain2.
    final List<String> result2 = new LinkedList<>();
    final String ocConf2 = idAndConfGenerator.generateConf();
    final String sinkConf2 = idAndConfGenerator.generateConf();

    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(ocConf2, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(sinkConf2, result2);
    final List<String> paths2 = mock(List.class);

    // Config vertices
    final ConfigVertex srcVertex2 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf));
    final ConfigVertex ocVertex2 = new ConfigVertex(ExecutionVertex.Type.OPERATOR_CHAIN, Arrays.asList(ocConf2));
    final ConfigVertex sinkVertex2 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf2));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple2 = generateSimpleDag(src2, operatorChain2, sink2,
        srcVertex2, ocVertex2, sinkVertex2);

    // Execute two queries
    // The query 1 and 2 should be merged and the following dag should be created:
    // src1 -> oc1 -> sink1
    //      -> oc2 -> sink2

    final String query1Id = "q1";
    final String query2Id = "q2";
    queryStarter.start(query1Id, dagTuple1.getKey(), paths1);
    queryStarter.start(query2Id, dagTuple2.getKey(), paths2);

    // Generate events for the merged query and check if the dag is executed correctly
    final String data1 = "Hello";
    src1.send(data1);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(1, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
    Assert.assertEquals(Arrays.asList(data1), result2);

    final String data2 = "World";
    src1.send(data2);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(1, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1, data2), result1);
    Assert.assertEquals(Arrays.asList(data1, data2), result2);

    // Src2 and 1 are the same, so the output emitter of src2 should be null
    try {
      src2.send(data1);
      Assert.fail("OutputEmitter should be null");
    } catch (final NullPointerException e) {
      // do nothing
    }

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    final DAG<ExecutionVertex, MISTEdge> mergedDag = dagTuple1.getValue();
    mergedDag.addVertex(operatorChain2);
    mergedDag.addVertex(sink2);
    mergedDag.addEdge(src1, operatorChain2, new MISTEdge(Direction.LEFT));
    mergedDag.addEdge(operatorChain2, sink2, new MISTEdge(Direction.LEFT));
    expectedDags.add(srcAndDagMap.get(sourceConf));
    Assert.assertEquals(expectedDags, executionDags.values());

    // Check queryIdConfigDagMap
    Assert.assertEquals(dagTuple1.getKey(), queryIdConfigDagMap.get(query1Id));
    Assert.assertEquals(dagTuple2.getKey(), queryIdConfigDagMap.get(query2Id));

    // Check srcAndDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, srcAndDagMap.get(sourceConf)));

    // Check executionVertexDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(src1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(operatorChain1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(sink1)));

    // They are merged, so src2, oc2 and sink2 should be included in dag1
    Assert.assertEquals(null, executionVertexDagMap.get(src2));
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(operatorChain2)));
    Assert.assertTrue(GraphUtils.compareTwoDag(mergedDag, executionVertexDagMap.get(sink2)));

    // Check configExecutionVertexMap
    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex1));
    Assert.assertEquals(operatorChain1, configExecutionVertexMap.get(ocVertex1));
    Assert.assertEquals(sink1, configExecutionVertexMap.get(sinkVertex1));

    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex2));
    Assert.assertEquals(operatorChain2, configExecutionVertexMap.get(ocVertex2));
    Assert.assertEquals(sink2, configExecutionVertexMap.get(sinkVertex2));

    // Check reference count
    Assert.assertEquals(2, (int)executionVertexCountMap.get(src1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(operatorChain1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink1));
    Assert.assertEquals(null, executionVertexCountMap.get(src2));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(operatorChain2));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink2));
  }

  /**
   * Test source that sends data to next operator chains.
   */
  final class TestSource implements PhysicalSource {
    private OutputEmitter outputEmitter;
    private final String id;
    private final String conf;

    TestSource(final String id,
               final String conf) {
      this.id = id;
      this.conf = conf;
    }

    @Override
    public void start() {
      // do nothing
    }

    /**
     * Send the data to the next operator chains.
     * @param data data
     * @param <T> data type
     */
    public <T> void send(final T data) {
      if (outputEmitter == null) {
        throw new NullPointerException("Output emitter is null");
      }
      outputEmitter.emitData(new MistDataEvent(data));
    }

    @Override
    public void close() throws Exception {
      // do nothing
    }

    @Override
    public Type getType() {
      return Type.SOURCE;
    }

    @Override
    public String getIdentifier() {
      return id;
    }

    @Override
    public void setOutputEmitter(final OutputEmitter emitter) {
      outputEmitter = emitter;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public String getConfiguration() {
      return conf;
    }
  }

  /**
   * Test sink that receives results and stores them to the list.
   * @param <T>
   */
  final class TestSink<T> implements Sink<T> {
    private final List<T> result;

    public TestSink(final List<T> result) {
      this.result = result;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void handle(final T input) {
      result.add(input);
    }
  }
}
