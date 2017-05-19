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
import edu.snu.mist.common.graph.AdjacentListDAG;
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Test whether MergeAwareQueryRemover removes queries correctly.
 */
public final class MergeAwareQueryRemoverTest {

  /**
   * Id and configuration generator.
   */
  private IdAndConfGenerator idAndConfGenerator;

  /**
   * The map that has the query id as a key and its configuration dag as a value.
   */
  private QueryIdConfigDagMap queryIdConfigDagMap;


  /**
   * Query remover.
   */
  private MergeAwareQueryRemover queryRemover;

  /**
   * The map that has the query id as a key and its execution dag as a value.
   */
  private SrcAndDagMap<String> srcAndDagMap;

  /**
   * The physical execution dags.
   */
  private ExecutionDags executionDags;

  /**
   * A map that has config vertex as a key and the corresponding execution vertex as a value.
   */
  private ConfigExecutionVertexMap configExecutionVertexMap;

  /**
   * A map that has an execution vertex as a key and the reference count number as a value.
   * The reference count number represents how many queries are sharing the execution vertex.
   */
  private ExecutionVertexCountMap executionVertexCountMap;

  /**
   * A map that has an execution vertex as a key and the dag that contains its vertex as a value.
   */
  private ExecutionVertexDagMap executionVertexDagMap;

  @Before
  public void setUp() throws InjectionException, IOException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    queryRemover = injector.getInstance(MergeAwareQueryRemover.class);
    srcAndDagMap = injector.getInstance(SrcAndDagMap.class);
    executionVertexCountMap = injector.getInstance(ExecutionVertexCountMap.class);
    executionDags = injector.getInstance(ExecutionDags.class);
    executionVertexDagMap = injector.getInstance(ExecutionVertexDagMap.class);
    configExecutionVertexMap = injector.getInstance(ConfigExecutionVertexMap.class);
    queryIdConfigDagMap = injector.getInstance(QueryIdConfigDagMap.class);
    idAndConfGenerator = new IdAndConfGenerator();
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
    final DAG<ConfigVertex, MISTEdge> dag = new AdjacentListDAG<>();
    dag.addVertex(srcVertex);
    dag.addVertex(ocVertex);
    dag.addVertex(sinkVertex);

    dag.addEdge(srcVertex, ocVertex, new MISTEdge(Direction.LEFT));
    dag.addEdge(ocVertex, sinkVertex, new MISTEdge(Direction.LEFT));

    final DAG<ExecutionVertex, MISTEdge> executionDag = new AdjacentListDAG<>();
    executionDag.addVertex(source);
    executionDag.addVertex(operatorChain);
    executionDag.addVertex(sink);

    executionDag.addEdge(source, operatorChain, new MISTEdge(Direction.LEFT));
    executionDag.addEdge(operatorChain, sink, new MISTEdge(Direction.LEFT));

    return new Tuple<>(dag, executionDag);
  }

  /**
   * Case 1: Remove a query when there is a single execution query.
   */
  @Test
  public void removeQueryFromSingleExecutionQuery() throws IOException, InjectionException {
    // Physical vertices
    final List<String> result = new LinkedList<>();
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

    final String queryId = "test-query";

    // Add execution dag to srcAndDagMap
    srcAndDagMap.put(sourceConf, dagTuple.getValue());
    // Add execution dag to queryIdConfigDagMap
    queryIdConfigDagMap.put(queryId, dagTuple.getKey());
    // ConfigExecutionVertexMap
    configExecutionVertexMap.put(srcVertex, source);
    configExecutionVertexMap.put(ocVertex, operatorChain);
    configExecutionVertexMap.put(sinkVertex, sink);
    // ExecutionDags
    executionDags.add(dagTuple.getValue());
    // ExecutionVertexCountMap
    executionVertexCountMap.put(source, 1);
    executionVertexCountMap.put(operatorChain, 1);
    executionVertexCountMap.put(sink, 1);
    // ExecutionVertexDagMap
    executionVertexDagMap.put(source, dagTuple.getValue());
    executionVertexDagMap.put(operatorChain, dagTuple.getValue());
    executionVertexDagMap.put(sink, dagTuple.getValue());


    // Delete the query
    queryRemover.deleteQuery(queryId);

    // Check srcAndDagMap
    Assert.assertEquals(0, srcAndDagMap.size());
    // Check queryIdConfigDagMap
    Assert.assertNull(queryIdConfigDagMap.get(queryId));
    // Check configExecutionVertexMap
    Assert.assertNull(configExecutionVertexMap.get(srcVertex));
    Assert.assertNull(configExecutionVertexMap.get(ocVertex));
    Assert.assertNull(configExecutionVertexMap.get(sinkVertex));
    // Check ExecutionDags
    Assert.assertEquals(0, executionDags.values().size());
    // Check ExecutionVertexCountMap
    Assert.assertNull(executionVertexCountMap.get(source));
    Assert.assertNull(executionVertexCountMap.get(operatorChain));
    Assert.assertNull(executionVertexCountMap.get(sink));
    // Check ExecutionVertexDagMap
    Assert.assertNull(executionVertexDagMap.get(source));
    Assert.assertNull(executionVertexDagMap.get(operatorChain));
    Assert.assertNull(executionVertexDagMap.get(sink));

    // Check if the source is closed
    try {
      source.send("data");
      Assert.fail("Source should be closed");
    } catch (final InvalidDataSendException e) {
      // do nothing
    }
  }

  /**
   * Case 2: Remove a query where there are two execution queries.
   */
  @Test
  public void removeQueryFromTwoExecutionQueriesTest() throws InjectionException, IOException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();

    // Physical vertices
    final String sourceConf1 = idAndConfGenerator.generateConf();
    final String ocConf1 = idAndConfGenerator.generateConf();
    final String sinkConf1 = idAndConfGenerator.generateConf();
    final TestSource src1 = generateSource(sourceConf1);
    final OperatorChain operatorChain1 = generateFilterOperatorChain(ocConf1, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(sinkConf1, result1);

    // Config vertices
    final ConfigVertex srcVertex1 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf1));
    final ConfigVertex ocVertex1 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(ocConf1));
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
    final PhysicalSink<String> sink2 = generateSink(sinkConf2, result1);

    // Config vertices
    final ConfigVertex srcVertex2 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(sourceConf2));
    final ConfigVertex ocVertex2 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(ocConf2));
    final ConfigVertex sinkVertex2 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf2));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple2 = generateSimpleDag(src2, operatorChain2, sink2,
        srcVertex2, ocVertex2, sinkVertex2);

    // Execute two queries
    final String query1Id = "q1";
    final String query2Id = "q2";

    // Add execution dag to srcAndDagMap
    srcAndDagMap.put(sourceConf1, dagTuple1.getValue());
    srcAndDagMap.put(sourceConf2, dagTuple2.getValue());

    // Add execution dag to queryIdConfigDagMap
    queryIdConfigDagMap.put(query1Id, dagTuple1.getKey());
    queryIdConfigDagMap.put(query2Id, dagTuple2.getKey());

    // ConfigExecutionVertexMap
    configExecutionVertexMap.put(srcVertex1, src1);
    configExecutionVertexMap.put(ocVertex1, operatorChain1);
    configExecutionVertexMap.put(sinkVertex1, sink1);
    configExecutionVertexMap.put(srcVertex2, src2);
    configExecutionVertexMap.put(ocVertex2, operatorChain2);
    configExecutionVertexMap.put(sinkVertex2, sink2);
    // ExecutionDags
    executionDags.add(dagTuple1.getValue());
    executionDags.add(dagTuple2.getValue());
    // ExecutionVertexCountMap
    executionVertexCountMap.put(src1, 1);
    executionVertexCountMap.put(operatorChain1, 1);
    executionVertexCountMap.put(sink1, 1);
    executionVertexCountMap.put(src2, 1);
    executionVertexCountMap.put(operatorChain2, 1);
    executionVertexCountMap.put(sink2, 1);
    // ExecutionVertexDagMap
    executionVertexDagMap.put(src1, dagTuple1.getValue());
    executionVertexDagMap.put(operatorChain1, dagTuple1.getValue());
    executionVertexDagMap.put(sink1, dagTuple1.getValue());
    executionVertexDagMap.put(src2, dagTuple2.getValue());
    executionVertexDagMap.put(operatorChain2, dagTuple2.getValue());
    executionVertexDagMap.put(sink2, dagTuple2.getValue());

    // Remove query2
    queryRemover.deleteQuery(query2Id);


    // Check srcAndDagMap
    Assert.assertEquals(1, srcAndDagMap.size());
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), srcAndDagMap.get(src1.getConfiguration())));
    // Check queryIdConfigDagMap
    Assert.assertNull(queryIdConfigDagMap.get(query2Id));
    Assert.assertEquals(dagTuple1.getKey(), queryIdConfigDagMap.get(query1Id));
    // Check configExecutionVertexMap
    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex1));
    Assert.assertEquals(operatorChain1, configExecutionVertexMap.get(ocVertex1));
    Assert.assertEquals(sink1, configExecutionVertexMap.get(sinkVertex1));
    Assert.assertNull(configExecutionVertexMap.get(srcVertex2));
    Assert.assertNull(configExecutionVertexMap.get(ocVertex2));
    Assert.assertNull(configExecutionVertexMap.get(sinkVertex2));
    // Check ExecutionDags
    Assert.assertEquals(1, executionDags.values().size());
    // Check ExecutionVertexCountMap
    Assert.assertEquals(1, (int)executionVertexCountMap.get(src1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(operatorChain1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink1));
    Assert.assertNull(executionVertexCountMap.get(src2));
    Assert.assertNull(executionVertexCountMap.get(operatorChain2));
    Assert.assertNull(executionVertexCountMap.get(sink2));
    // Check ExecutionVertexDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(src1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(operatorChain1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(sink1)));
    Assert.assertNull(executionVertexDagMap.get(src2));
    Assert.assertNull(executionVertexDagMap.get(operatorChain2));
    Assert.assertNull(executionVertexDagMap.get(sink2));

    // Check if the source is closed
    try {
      src2.send("data");
      Assert.fail("Source should be closed");
    } catch (final InvalidDataSendException e) {
      // do nothing
    }
  }

  /**
   * Case 3: Remove a query where two queries are merged.
   * Query1: src1 -> oc1 -> sink1
   * Query2: src2 -> oc2 -> sink2
   * Merged: src1 -> oc1 -> sink1
   *                     -> sink2
   * After removing query2:
   *         src1 -> oc1 -> sink1
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  @Test
  public void removeQueryFromMergedExecutionQueriesTest()
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
    final ConfigVertex ocVertex1 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(operatorConf));
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
    final ConfigVertex ocVertex2 = new ConfigVertex(ExecutionVertex.Type.SOURCE, Arrays.asList(operatorConf));
    final ConfigVertex sinkVertex2 = new ConfigVertex(ExecutionVertex.Type.SINK, Arrays.asList(sinkConf2));

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, DAG<ExecutionVertex, MISTEdge>>
        dagTuple2 = generateSimpleDag(src2, operatorChain2, sink2,
        srcVertex2, ocVertex2, sinkVertex2);

    final String query1Id = "q1";
    final String query2Id = "q2";

    // Merged dag
    final DAG<ExecutionVertex, MISTEdge> mergedDag = dagTuple1.getValue();
    mergedDag.addVertex(sink2);
    mergedDag.addEdge(operatorChain1, sink2, new MISTEdge(Direction.LEFT));

    // Add execution dag to srcAndDagMap
    srcAndDagMap.put(sourceConf, mergedDag);

    // Add execution dag to queryIdConfigDagMap
    queryIdConfigDagMap.put(query1Id, dagTuple1.getKey());
    queryIdConfigDagMap.put(query2Id, dagTuple2.getKey());

    // ConfigExecutionVertexMap
    configExecutionVertexMap.put(srcVertex1, src1);
    configExecutionVertexMap.put(ocVertex1, operatorChain1);
    configExecutionVertexMap.put(sinkVertex1, sink1);
    configExecutionVertexMap.put(srcVertex2, src1);
    configExecutionVertexMap.put(ocVertex2, operatorChain1);
    configExecutionVertexMap.put(sinkVertex2, sink2);
    // ExecutionDags
    executionDags.add(mergedDag);
    // ExecutionVertexCountMap
    executionVertexCountMap.put(src1, 2);
    executionVertexCountMap.put(operatorChain1, 2);
    executionVertexCountMap.put(sink1, 1);
    executionVertexCountMap.put(sink2, 1);
    // ExecutionVertexDagMap
    executionVertexDagMap.put(src1, mergedDag);
    executionVertexDagMap.put(operatorChain1, mergedDag);
    executionVertexDagMap.put(sink1, mergedDag);
    executionVertexDagMap.put(sink2, mergedDag);

    // Remove query2
    queryRemover.deleteQuery(query2Id);

    // Check srcAndDagMap
    Assert.assertEquals(1, srcAndDagMap.size());
    Assert.assertEquals(dagTuple1.getValue(), srcAndDagMap.get(src1.getConfiguration()));
    // Check queryIdConfigDagMap
    Assert.assertNull(queryIdConfigDagMap.get(query2Id));
    Assert.assertEquals(dagTuple1.getKey(), queryIdConfigDagMap.get(query1Id));
    // Check configExecutionVertexMap
    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex1));
    Assert.assertEquals(operatorChain1, configExecutionVertexMap.get(ocVertex1));
    Assert.assertEquals(sink1, configExecutionVertexMap.get(sinkVertex1));
    Assert.assertNull(configExecutionVertexMap.get(srcVertex2));
    Assert.assertNull(configExecutionVertexMap.get(ocVertex2));
    Assert.assertNull(configExecutionVertexMap.get(sinkVertex2));
    // Check ExecutionDags
    Assert.assertEquals(1, executionDags.values().size());
    // Check ExecutionVertexCountMap
    Assert.assertEquals(1, (int)executionVertexCountMap.get(src1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(operatorChain1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink1));
    Assert.assertNull(executionVertexCountMap.get(src2));
    Assert.assertNull(executionVertexCountMap.get(operatorChain2));
    Assert.assertNull(executionVertexCountMap.get(sink2));
    // Check ExecutionVertexDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(src1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(operatorChain1)));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue(), executionVertexDagMap.get(sink1)));
    Assert.assertNull(executionVertexDagMap.get(src2));
    Assert.assertNull(executionVertexDagMap.get(operatorChain2));
    Assert.assertNull(executionVertexDagMap.get(sink2));

    // Check if the execution dag is changed correctly
    final Map<ExecutionVertex, MISTEdge> oc1Edges = mergedDag.getEdges(operatorChain1);
    Assert.assertEquals(1, oc1Edges.size());
    Assert.assertEquals(sink1, oc1Edges.keySet().iterator().next());
  }

  /**
   * An exception for deleting test.
   */
  final class InvalidDataSendException extends RuntimeException {
    public InvalidDataSendException(final String msg) {
      super(msg);
    }
  }

  /**
   * Test source that sends data to next operator chains.
   */
  final class TestSource implements PhysicalSource {
    private OutputEmitter outputEmitter;
    private final String id;
    private final String conf;
    private boolean closed = false;

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
      if (closed) {
        throw new InvalidDataSendException("exception");
      } else {
        outputEmitter.emitData(new MistDataEvent(data));
      }
    }

    @Override
    public void close() throws Exception {
      closed = true;
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
