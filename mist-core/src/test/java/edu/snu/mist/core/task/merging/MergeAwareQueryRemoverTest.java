/*
 * Copyright (C) 2018 Seoul National University
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
import edu.snu.mist.common.sources.EventGenerator;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

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
  private SrcAndDagMap<Map<String, String>> srcAndDagMap;

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

  /**
   * Atomic ID used for generating ConfigVertex Ids.
   */
  private AtomicLong configVertexId;

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
    configVertexId = new AtomicLong();
  }

  /**
   * Get a test source with the configuration.
   * @param conf source configuration
   * @return test source
   */
  private TestSource generateSource(final Map<String, String> conf) {
    return new TestSource(idAndConfGenerator.generateId(), conf);
  }

  /**
   * Get a simple operator chain that has a filter operator.
   * @param conf configuration of the operator
   * @return operator chain
   */
  private PhysicalOperator generateFilterOperator(final Map<String, String> conf,
                                                  final MISTPredicate<String> predicate) {
    final PhysicalOperator filterOp = new DefaultPhysicalOperatorImpl(idAndConfGenerator.generateId(),
        conf, new FilterOperator<>(predicate));
    return filterOp;
  }

  /**
   * Get a sink that stores the outputs to the list.
   * @param conf configuration of the sink
   * @param result list for storing outputs
   * @return sink
   */
  private PhysicalSink<String> generateSink(final Map<String, String> conf,
                                            final List<String> result) {
    return new PhysicalSinkImpl<>(idAndConfGenerator.generateId(), conf, new TestSink<>(result));
  }

  /**
   * Generate a simple query that has the following structure: src -> operator chain -> sink.
   * @param source source
   * @param physicalOperator operator chain
   * @param sink sink
   * @return dag
   */
  private Tuple<DAG<ConfigVertex, MISTEdge>, ExecutionDag> generateSimpleDag(
      final TestSource source,
      final PhysicalOperator physicalOperator,
      final PhysicalSink<String> sink,
      final ConfigVertex srcVertex,
      final ConfigVertex ocVertex,
      final ConfigVertex sinkVertex) throws IOException {
    // Create DAG
    final DAG<ConfigVertex, MISTEdge> dag = new AdjacentListDAG<>();
    dag.addVertex(srcVertex);
    dag.addVertex(ocVertex);
    dag.addVertex(sinkVertex);

    dag.addEdge(srcVertex, ocVertex, new MISTEdge(Direction.LEFT));
    dag.addEdge(ocVertex, sinkVertex, new MISTEdge(Direction.LEFT));

    final DAG<ExecutionVertex, MISTEdge> newDag = new AdjacentListDAG<>();
    newDag.addVertex(source);
    newDag.addVertex(physicalOperator);
    newDag.addVertex(sink);

    newDag.addEdge(source, physicalOperator, new MISTEdge(Direction.LEFT));
    newDag.addEdge(physicalOperator, sink, new MISTEdge(Direction.LEFT));

    final ExecutionDag executionDag = new ExecutionDag(newDag);
    return new Tuple<>(dag, executionDag);
  }

  /**
   * Case 1: Remove a query when there is a single execution query.
   */
  @Test
  public void removeQueryFromSingleExecutionQuery() throws IOException {
    // Physical vertices
    final List<String> result = new LinkedList<>();
    final Map<String, String> sourceConf = idAndConfGenerator.generateConf();
    final Map<String, String> ocConf = idAndConfGenerator.generateConf();
    final Map<String, String> sinkConf = idAndConfGenerator.generateConf();
    final TestSource source = generateSource(sourceConf);
    final PhysicalOperator physicalOp = generateFilterOperator(ocConf, (s) -> true);
    final PhysicalSink<String> sink = generateSink(sinkConf, result);

    // Config vertices
    final ConfigVertex srcVertex = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SOURCE, sourceConf);
    final ConfigVertex ocVertex = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.OPERATOR, ocConf);
    final ConfigVertex sinkVertex = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SINK, sinkConf);

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, ExecutionDag>
        dagTuple = generateSimpleDag(source, physicalOp, sink,
        srcVertex, ocVertex, sinkVertex);

    final String queryId = "test-query";

    // Add execution dag to srcAndDagMap
    srcAndDagMap.put(sourceConf, dagTuple.getValue());
    // Add execution dag to queryIdConfigDagMap
    queryIdConfigDagMap.put(queryId, dagTuple.getKey());
    // ConfigExecutionVertexMap
    configExecutionVertexMap.put(srcVertex, source);
    configExecutionVertexMap.put(ocVertex, physicalOp);
    configExecutionVertexMap.put(sinkVertex, sink);
    // ExecutionDags
    executionDags.add(dagTuple.getValue());
    // ExecutionVertexCountMap
    executionVertexCountMap.put(source, 1);
    executionVertexCountMap.put(physicalOp, 1);
    executionVertexCountMap.put(sink, 1);
    // ExecutionVertexDagMap
    executionVertexDagMap.put(source, dagTuple.getValue());
    executionVertexDagMap.put(physicalOp, dagTuple.getValue());
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
    Assert.assertNull(executionVertexCountMap.get(physicalOp));
    Assert.assertNull(executionVertexCountMap.get(sink));
    // Check ExecutionVertexDagMap
    Assert.assertNull(executionVertexDagMap.get(source));
    Assert.assertNull(executionVertexDagMap.get(physicalOp));
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
    final Map<String, String> sourceConf1 = idAndConfGenerator.generateConf();
    final Map<String, String> ocConf1 = idAndConfGenerator.generateConf();
    final Map<String, String> sinkConf1 = idAndConfGenerator.generateConf();
    final TestSource src1 = generateSource(sourceConf1);
    final PhysicalOperator physicalOp1 = generateFilterOperator(ocConf1, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(sinkConf1, result1);

    // Config vertices
    final ConfigVertex srcVertex1 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SOURCE, sourceConf1);
    final ConfigVertex ocVertex1 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.OPERATOR, ocConf1);
    final ConfigVertex sinkVertex1 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SINK, sinkConf1);

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, ExecutionDag>
        dagTuple1 = generateSimpleDag(src1, physicalOp1, sink1,
        srcVertex1, ocVertex1, sinkVertex1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 is different from that of src1.
    final List<String> result2 = new LinkedList<>();
    final List<String> paths2 = mock(List.class);

    // Physical vertices
    final Map<String, String> sourceConf2 = idAndConfGenerator.generateConf();
    final Map<String, String> ocConf2 = idAndConfGenerator.generateConf();
    final Map<String, String> sinkConf2 = idAndConfGenerator.generateConf();
    final TestSource src2 = generateSource(sourceConf2);
    final PhysicalOperator physicalOp2 = generateFilterOperator(ocConf2, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(sinkConf2, result1);

    // Config vertices
    final ConfigVertex srcVertex2 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SOURCE, sourceConf2);
    final ConfigVertex ocVertex2 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.OPERATOR, ocConf2);
    final ConfigVertex sinkVertex2 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SINK, sinkConf2);

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, ExecutionDag>
        dagTuple2 = generateSimpleDag(src2, physicalOp2, sink2,
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
    configExecutionVertexMap.put(ocVertex1, physicalOp1);
    configExecutionVertexMap.put(sinkVertex1, sink1);
    configExecutionVertexMap.put(srcVertex2, src2);
    configExecutionVertexMap.put(ocVertex2, physicalOp2);
    configExecutionVertexMap.put(sinkVertex2, sink2);
    // ExecutionDags
    executionDags.add(dagTuple1.getValue());
    executionDags.add(dagTuple2.getValue());
    // ExecutionVertexCountMap
    executionVertexCountMap.put(src1, 1);
    executionVertexCountMap.put(physicalOp1, 1);
    executionVertexCountMap.put(sink1, 1);
    executionVertexCountMap.put(src2, 1);
    executionVertexCountMap.put(physicalOp2, 1);
    executionVertexCountMap.put(sink2, 1);
    // ExecutionVertexDagMap
    executionVertexDagMap.put(src1, dagTuple1.getValue());
    executionVertexDagMap.put(physicalOp1, dagTuple1.getValue());
    executionVertexDagMap.put(sink1, dagTuple1.getValue());
    executionVertexDagMap.put(src2, dagTuple2.getValue());
    executionVertexDagMap.put(physicalOp2, dagTuple2.getValue());
    executionVertexDagMap.put(sink2, dagTuple2.getValue());

    // Remove query2
    queryRemover.deleteQuery(query2Id);


    // Check srcAndDagMap
    Assert.assertEquals(1, srcAndDagMap.size());
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue().getDag(),
        srcAndDagMap.get(src1.getConfiguration()).getDag()));
    // Check queryIdConfigDagMap
    Assert.assertNull(queryIdConfigDagMap.get(query2Id));
    Assert.assertEquals(dagTuple1.getKey(), queryIdConfigDagMap.get(query1Id));
    // Check configExecutionVertexMap
    Assert.assertEquals(src1, configExecutionVertexMap.get(srcVertex1));
    Assert.assertEquals(physicalOp1, configExecutionVertexMap.get(ocVertex1));
    Assert.assertEquals(sink1, configExecutionVertexMap.get(sinkVertex1));
    Assert.assertNull(configExecutionVertexMap.get(srcVertex2));
    Assert.assertNull(configExecutionVertexMap.get(ocVertex2));
    Assert.assertNull(configExecutionVertexMap.get(sinkVertex2));
    // Check ExecutionDags
    Assert.assertEquals(1, executionDags.values().size());
    // Check ExecutionVertexCountMap
    Assert.assertEquals(1, (int)executionVertexCountMap.get(src1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(physicalOp1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink1));
    Assert.assertNull(executionVertexCountMap.get(src2));
    Assert.assertNull(executionVertexCountMap.get(physicalOp2));
    Assert.assertNull(executionVertexCountMap.get(sink2));
    // Check ExecutionVertexDagMap
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue().getDag(),
        executionVertexDagMap.get(src1).getDag()));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue().getDag(),
        executionVertexDagMap.get(physicalOp1).getDag()));
    Assert.assertTrue(GraphUtils.compareTwoDag(dagTuple1.getValue().getDag(),
        executionVertexDagMap.get(sink1).getDag()));
    Assert.assertNull(executionVertexDagMap.get(src2));
    Assert.assertNull(executionVertexDagMap.get(physicalOp2));
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
    final Map<String, String> sourceConf = idAndConfGenerator.generateConf();
    final Map<String, String> operatorConf = idAndConfGenerator.generateConf();
    final TestSource src1 = generateSource(sourceConf);
    final Map<String, String> sinkConf1 = idAndConfGenerator.generateConf();
    final PhysicalOperator physicalOp1 = generateFilterOperator(operatorConf, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(sinkConf1, result1);

    // Config vertices
    final ConfigVertex srcVertex1 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SOURCE, sourceConf);
    final ConfigVertex ocVertex1 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.OPERATOR, operatorConf);
    final ConfigVertex sinkVertex1 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SINK, sinkConf1);
    final List<String> paths1 = mock(List.class);

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, ExecutionDag>
        dagTuple1 = generateSimpleDag(src1, physicalOp1, sink1,
        srcVertex1, ocVertex1, sinkVertex1);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 and operatorChain2 is same as that of src1 and operatorChain2.
    final List<String> result2 = new LinkedList<>();
    final Map<String, String> sinkConf2 = idAndConfGenerator.generateConf();
    final TestSource src2 = generateSource(sourceConf);
    final PhysicalOperator physicalOp2 = generateFilterOperator(operatorConf, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(sinkConf2, result2);
    final List<String> paths2 = mock(List.class);

    // Config vertices
    final ConfigVertex srcVertex2 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SOURCE, sourceConf);
    final ConfigVertex ocVertex2 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SOURCE, operatorConf);
    final ConfigVertex sinkVertex2 = new ConfigVertex(Long.toString(configVertexId.getAndIncrement()),
        ExecutionVertex.Type.SINK, sinkConf2);

    // Create dag
    final Tuple<DAG<ConfigVertex, MISTEdge>, ExecutionDag>
        dagTuple2 = generateSimpleDag(src2, physicalOp2, sink2,
        srcVertex2, ocVertex2, sinkVertex2);

    final String query1Id = "q1";
    final String query2Id = "q2";

    // Merged dag
    final ExecutionDag mergedExecutionDag = dagTuple1.getValue();
    final DAG<ExecutionVertex, MISTEdge> dag = mergedExecutionDag.getDag();
    dag.addVertex(sink2);
    dag.addEdge(physicalOp1, sink2, new MISTEdge(Direction.LEFT));

    // Add execution dag to srcAndDagMap
    srcAndDagMap.put(sourceConf, mergedExecutionDag);

    // Add execution dag to queryIdConfigDagMap
    queryIdConfigDagMap.put(query1Id, dagTuple1.getKey());
    queryIdConfigDagMap.put(query2Id, dagTuple2.getKey());

    // ConfigExecutionVertexMap
    configExecutionVertexMap.put(srcVertex1, src1);
    configExecutionVertexMap.put(ocVertex1, physicalOp1);
    configExecutionVertexMap.put(sinkVertex1, sink1);
    configExecutionVertexMap.put(srcVertex2, src1);
    configExecutionVertexMap.put(ocVertex2, physicalOp1);
    configExecutionVertexMap.put(sinkVertex2, sink2);
    // ExecutionDags
    executionDags.add(mergedExecutionDag);
    // ExecutionVertexCountMap
    executionVertexCountMap.put(src1, 2);
    executionVertexCountMap.put(physicalOp1, 2);
    executionVertexCountMap.put(sink1, 1);
    executionVertexCountMap.put(sink2, 1);
    // ExecutionVertexDagMap
    executionVertexDagMap.put(src1, mergedExecutionDag);
    executionVertexDagMap.put(physicalOp1, mergedExecutionDag);
    executionVertexDagMap.put(sink1, mergedExecutionDag);
    executionVertexDagMap.put(sink2, mergedExecutionDag);

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
    Assert.assertEquals(physicalOp1, configExecutionVertexMap.get(ocVertex1));
    Assert.assertEquals(sink1, configExecutionVertexMap.get(sinkVertex1));
    Assert.assertNull(configExecutionVertexMap.get(srcVertex2));
    Assert.assertNull(configExecutionVertexMap.get(ocVertex2));
    Assert.assertNull(configExecutionVertexMap.get(sinkVertex2));
    // Check ExecutionDags
    Assert.assertEquals(1, executionDags.values().size());
    // Check ExecutionVertexCountMap
    Assert.assertEquals(1, (int)executionVertexCountMap.get(src1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(physicalOp1));
    Assert.assertEquals(1, (int)executionVertexCountMap.get(sink1));
    Assert.assertNull(executionVertexCountMap.get(src2));
    Assert.assertNull(executionVertexCountMap.get(physicalOp2));
    Assert.assertNull(executionVertexCountMap.get(sink2));
    // Check ExecutionVertexDagMap
    final DAG<ExecutionVertex, MISTEdge> dag1 = dagTuple1.getValue().getDag();
    Assert.assertTrue(GraphUtils.compareTwoDag(dag1,
        executionVertexDagMap.get(src1).getDag()));
    Assert.assertTrue(GraphUtils.compareTwoDag(dag1,
        executionVertexDagMap.get(physicalOp1).getDag()));
    Assert.assertTrue(GraphUtils.compareTwoDag(dag1,
        executionVertexDagMap.get(sink1).getDag()));
    Assert.assertNull(executionVertexDagMap.get(src2));
    Assert.assertNull(executionVertexDagMap.get(physicalOp2));
    Assert.assertNull(executionVertexDagMap.get(sink2));

    // Check if the execution dag is changed correctly
    final Map<ExecutionVertex, MISTEdge> oc1Edges = mergedExecutionDag.getDag().getEdges(physicalOp1);
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
    private SourceOutputEmitter outputEmitter;
    private final String id;
    private final Map<String, String> conf;
    private boolean closed = false;

    TestSource(final String id,
               final Map<String, String> conf) {
      this.id = id;
      this.conf = conf;
    }

    @Override
    public void start() {
      // do nothing
    }

    @Override
    public EventGenerator getEventGenerator() {
      return null;
    }

    @Override
    public SourceOutputEmitter getSourceOutputEmitter() {
      return outputEmitter;
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
      outputEmitter = (SourceOutputEmitter)emitter;
    }

    @Override
    public String getId() {
      return id;
    }

    @Override
    public Map<String, String> getConfiguration() {
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
