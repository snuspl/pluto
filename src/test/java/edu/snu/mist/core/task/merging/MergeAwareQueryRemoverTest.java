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
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Test whether MergeAwareQueryRemover removes queries correctly.
 */
public final class MergeAwareQueryRemoverTest {

  /**
   * Id and configuration generator.
   */
  private IdAndConfGenerator idAndConfGenerator;

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
   * The map that holds execution plans of queries.
   */
  private ExecutionPlanDagMap executionPlanDagMap;

  /**
   * Vertex info map.
   */
  private VertexInfoMap vertexInfoMap;

  @Before
  public void setUp() throws InjectionException, IOException {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    queryRemover = injector.getInstance(MergeAwareQueryRemover.class);
    srcAndDagMap = injector.getInstance(SrcAndDagMap.class);
    executionPlanDagMap = injector.getInstance(ExecutionPlanDagMap.class);
    executionDags = injector.getInstance(ExecutionDags.class);
    vertexInfoMap = injector.getInstance(VertexInfoMap.class);
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
  private DAG<ExecutionVertex, MISTEdge> generateSimpleQuery(final TestSource source,
                                                             final OperatorChain operatorChain,
                                                             final PhysicalSink<String> sink) {
    // Create DAG
    final DAG<ExecutionVertex, MISTEdge> dag = new AdjacentListDAG<>();
    dag.addVertex(source);
    dag.addVertex(operatorChain);
    dag.addVertex(sink);
    dag.addEdge(source, operatorChain, new MISTEdge(Direction.LEFT));
    dag.addEdge(operatorChain, sink, new MISTEdge(Direction.LEFT));
    return dag;
  }

  /**
   * Case 1: Remove a query when there is a single execution query.
   */
  @Test
  public void removeQueryFromSingleExecutionQuery() {
    final List<String> result = new LinkedList<>();
    final String sourceConf = idAndConfGenerator.generateConf();
    final TestSource source = generateSource(sourceConf);
    final OperatorChain operatorChain = generateFilterOperatorChain(idAndConfGenerator.generateConf(), (s) -> true);
    final PhysicalSink<String> sink = generateSink(idAndConfGenerator.generateConf(), result);
    final DAG<ExecutionVertex, MISTEdge> executionDag = generateSimpleQuery(source, operatorChain, sink);
    final String queryId = "test-query";

    // Add query info to srcAndDagMap, executionPlanMap, and vertexInfoMap
    final DAG<ExecutionVertex, MISTEdge> plan = new AdjacentListDAG<>();
    GraphUtils.copy(executionDag, plan);
    srcAndDagMap.put(sourceConf, executionDag);
    executionPlanDagMap.put(queryId, plan);
    final Collection<ExecutionVertex> vertices = plan.getVertices();
    for (final ExecutionVertex ev : vertices) {
      vertexInfoMap.put(ev, new VertexInfo(executionDag, ev));
    }

    // Delete the query
    queryRemover.deleteQuery(queryId);

    // Check
    Assert.assertEquals(0, srcAndDagMap.size());
    Assert.assertEquals(0, executionDags.values().size());
    Assert.assertNull(executionPlanDagMap.get(queryId));
    Assert.assertNull(vertexInfoMap.get(source));
    Assert.assertNull(vertexInfoMap.get(operatorChain));
    Assert.assertNull(vertexInfoMap.get(sink));

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
  public void removeQueryFromTwoExecutionQueriesTest() throws InjectionException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final TestSource src1 = generateSource(idAndConfGenerator.generateConf());
    final OperatorChain operatorChain1 = generateFilterOperatorChain(idAndConfGenerator.generateConf(), (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(idAndConfGenerator.generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);
    final String query1Id = "test-query1";

    // Add query info to srcAndDagMap, executionPlanMap, and vertexInfoMap
    final DAG<ExecutionVertex, MISTEdge> plan1 = new AdjacentListDAG<>();
    GraphUtils.copy(query1, plan1);
    srcAndDagMap.put(src1.getConfiguration(), query1);
    executionDags.add(query1);
    executionPlanDagMap.put(query1Id, plan1);
    final Collection<ExecutionVertex> vertices1 = plan1.getVertices();
    for (final ExecutionVertex ev : vertices1) {
      vertexInfoMap.put(ev, new VertexInfo(query1, ev));
    }

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 is different from that of src1.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(idAndConfGenerator.generateConf());
    final OperatorChain operatorChain2 = generateFilterOperatorChain(idAndConfGenerator.generateConf(), (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(idAndConfGenerator.generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);
    final String query2Id = "test-query2";

    // Add query info to srcAndDagMap, executionPlanMap, and vertexInfoMap
    final DAG<ExecutionVertex, MISTEdge> plan2 = new AdjacentListDAG<>();
    GraphUtils.copy(query2, plan2);
    srcAndDagMap.put(src2.getConfiguration(), query2);
    executionPlanDagMap.put(query2Id, plan2);
    final Collection<ExecutionVertex> vertices2 = plan2.getVertices();
    for (final ExecutionVertex ev : vertices2) {
      vertexInfoMap.put(ev, new VertexInfo(query2, ev));
    }

    // Remove query2
    queryRemover.deleteQuery(query2Id);

    // Check
    Assert.assertEquals(1, srcAndDagMap.size());
    Assert.assertEquals(query1, srcAndDagMap.get(src1.getConfiguration()));
    Assert.assertEquals(plan1, executionPlanDagMap.get(query1Id));
    Assert.assertEquals(src1, vertexInfoMap.get(src1).getPhysicalExecutionVertex());
    Assert.assertEquals(operatorChain1, vertexInfoMap.get(operatorChain1).getPhysicalExecutionVertex());
    Assert.assertEquals(sink1, vertexInfoMap.get(sink1).getPhysicalExecutionVertex());

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(query1);
    Assert.assertEquals(expectedDags, executionDags.values());

    Assert.assertNull(executionPlanDagMap.get(query2Id));
    Assert.assertNull(vertexInfoMap.get(src2));
    Assert.assertNull(vertexInfoMap.get(operatorChain2));
    Assert.assertNull(vertexInfoMap.get(sink2));

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
    final OperatorChain operatorChain1 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(idAndConfGenerator.generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);
    final DAG<ExecutionVertex, MISTEdge> query1Plan = new AdjacentListDAG<>();
    final String query1Id = "test-query1";
    GraphUtils.copy(query1, query1Plan);

    // Add query info to srcAndDagMap, executionPlanMap, and vertexInfoMap
    srcAndDagMap.put(src1.getConfiguration(), query1);
    executionPlanDagMap.put(query1Id, query1Plan);
    executionDags.add(query1);
    final VertexInfo src1VertexInfo = new VertexInfo(query1, src1);
    final VertexInfo oc1VertexInfo = new VertexInfo(query1, operatorChain1);
    final VertexInfo sink1VertexInfo = new VertexInfo(query1, sink1);
    vertexInfoMap.put(src1, src1VertexInfo);
    vertexInfoMap.put(operatorChain1, oc1VertexInfo);
    vertexInfoMap.put(sink1, sink1VertexInfo);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 and operatorChain2 is same as that of src1 and operatorChain2.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(idAndConfGenerator.generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);
    final DAG<ExecutionVertex, MISTEdge> query2Plan = new AdjacentListDAG<>();
    final String query2Id = "test-query2";
    GraphUtils.copy(query2, query2Plan);
    // Adjust the execution dag of query1 in order to reflect the merging
    query1.addVertex(sink2);
    query1.addEdge(operatorChain1, sink2, query2.getEdges(operatorChain2).get(sink2));

    // Add query info to srcAndDagMap, executionPlanMap, and vertexInfoMap
    srcAndDagMap.put(src2.getConfiguration(), query1);
    executionPlanDagMap.put(query2Id, query2Plan);
    vertexInfoMap.put(src2, src1VertexInfo);
    src1VertexInfo.setRefCount(src1VertexInfo.getRefCount() + 1);
    vertexInfoMap.put(operatorChain2, oc1VertexInfo);
    oc1VertexInfo.setRefCount(oc1VertexInfo.getRefCount() + 1);
    vertexInfoMap.put(sink2, new VertexInfo(query1, sink2));

    // Remove query2
    queryRemover.deleteQuery(query2Id);

    // Check
    Assert.assertEquals(1, src1VertexInfo.getRefCount());
    Assert.assertEquals(1, oc1VertexInfo.getRefCount());
    Assert.assertEquals(1, sink1VertexInfo.getRefCount());

    Assert.assertEquals(1, srcAndDagMap.size());
    Assert.assertEquals(query1, srcAndDagMap.get(src1.getConfiguration()));
    Assert.assertEquals(query1Plan, executionPlanDagMap.get(query1Id));
    Assert.assertEquals(src1, vertexInfoMap.get(src1).getPhysicalExecutionVertex());
    Assert.assertEquals(operatorChain1, vertexInfoMap.get(operatorChain1).getPhysicalExecutionVertex());
    Assert.assertEquals(sink1, vertexInfoMap.get(sink1).getPhysicalExecutionVertex());

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(query1);
    Assert.assertEquals(expectedDags, executionDags.values());

    Assert.assertNull(executionPlanDagMap.get(query2Id));
    Assert.assertNull(vertexInfoMap.get(src2));
    Assert.assertNull(vertexInfoMap.get(operatorChain2));
    Assert.assertNull(vertexInfoMap.get(sink2));

    // Check if the execution dag is changed correctly
    final Map<ExecutionVertex, MISTEdge> oc1Edges = query1.getEdges(operatorChain1);
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
