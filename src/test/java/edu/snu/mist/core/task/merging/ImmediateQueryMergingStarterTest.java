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
 * Test whether ImmediateQueryMergingStarter merges and starts the submitted queries correctly.
 */
public final class ImmediateQueryMergingStarterTest {

  private IdAndConfGenerator idAndConfGenerator;

  @Before
  public void setUp() throws InjectionException, IOException {
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
   * Check the reference count of the vertices of a DAG.
   * @param dag dag
   * @param vertexInfoMap vertex info map that contains the reference count
   * @param refCount expected reference count
   */
  private void checkReferenceCountOfExecutionVertices(final DAG<ExecutionVertex, MISTEdge> dag,
                                                      final VertexInfoMap vertexInfoMap,
                                                      final int refCount) {
    for (final ExecutionVertex ev : dag.getVertices()) {
      Assert.assertEquals(refCount, vertexInfoMap.get(ev).getRefCount());
    }
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
  public void singleQueryMergingTest() throws InjectionException {
    final List<String> result = new LinkedList<>();
    final String sourceConf = idAndConfGenerator.generateConf();
    final TestSource source = generateSource(sourceConf);
    final OperatorChain operatorChain = generateFilterOperatorChain(idAndConfGenerator.generateConf(), (s) -> true);
    final PhysicalSink<String> sink = generateSink(idAndConfGenerator.generateConf(), result);
    final DAG<ExecutionVertex, MISTEdge> query = generateSimpleQuery(source, operatorChain, sink);
    // Execute the query 1
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final ImmediateQueryMergingStarter starter = injector.getInstance(ImmediateQueryMergingStarter.class);
    final SrcAndDagMap<String> srcAndDagMap = injector.getInstance(SrcAndDagMap.class);
    final ExecutionPlanDagMap executionPlanDagMap = injector.getInstance(ExecutionPlanDagMap.class);
    final ExecutionDags executionDags = injector.getInstance(ExecutionDags.class);
    final VertexInfoMap vertexInfoMap = injector.getInstance(VertexInfoMap.class);
    starter.start("q1", query);

    // Generate events for the query and check if the dag is executed correctly
    final String data1 = "Hello";
    source.send(data1);
    Assert.assertEquals(Arrays.asList(), result);
    Assert.assertEquals(1, operatorChain.numberOfEvents());
    Assert.assertEquals(true, operatorChain.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result);
    Assert.assertEquals(query, srcAndDagMap.get(sourceConf));

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(query);
    Assert.assertEquals(expectedDags, executionDags.values());
    // Check reference count of the execution vertices
    Assert.assertEquals(query, executionPlanDagMap.get("q1"));
    checkReferenceCountOfExecutionVertices(query, vertexInfoMap, 1);
  }

  /**
   * Case 2: Merging two queries that have different sources.
   */
  @Test
  public void mergingDifferentSourceQueriesOneGroupTest() throws InjectionException {
    // Create a query 1:
    // src1 -> oc1 -> sink1
    final List<String> result1 = new LinkedList<>();
    final TestSource src1 = generateSource(idAndConfGenerator.generateConf());
    final OperatorChain operatorChain1 = generateFilterOperatorChain(idAndConfGenerator.generateConf(), (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(idAndConfGenerator.generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);
    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 is different from that of src1.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(idAndConfGenerator.generateConf());
    final OperatorChain operatorChain2 = generateFilterOperatorChain(idAndConfGenerator.generateConf(), (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(idAndConfGenerator.generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);

    // Execute two queries
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final ImmediateQueryMergingStarter starter = injector.getInstance(ImmediateQueryMergingStarter.class);
    final SrcAndDagMap<String> srcAndDagMap = injector.getInstance(SrcAndDagMap.class);
    final ExecutionDags executionDags = injector.getInstance(ExecutionDags.class);
    final ExecutionPlanDagMap executionPlanDagMap = injector.getInstance(ExecutionPlanDagMap.class);
    final VertexInfoMap vertexInfoMap = injector.getInstance(VertexInfoMap.class);
    starter.start("q1", query1);
    starter.start("q2", query2);

    // Check
    Assert.assertEquals(2, srcAndDagMap.size());
    Assert.assertEquals(query1, srcAndDagMap.get(src1.getConfiguration()));
    Assert.assertEquals(query2, srcAndDagMap.get(src2.getConfiguration()));

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

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(query1);
    expectedDags.add(query2);
    Assert.assertEquals(expectedDags, executionDags.values());

    // Check reference count of the execution vertices
    Assert.assertEquals(query1, executionPlanDagMap.get("q1"));
    Assert.assertEquals(query2, executionPlanDagMap.get("q2"));
    checkReferenceCountOfExecutionVertices(query1, vertexInfoMap, 1);
    checkReferenceCountOfExecutionVertices(query2, vertexInfoMap, 1);
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
    final OperatorChain operatorChain1 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink1 = generateSink(idAndConfGenerator.generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);
    final DAG<ExecutionVertex, MISTEdge> query1Plan = new AdjacentListDAG<>();
    GraphUtils.copy(query1, query1Plan);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 and operatorChain2 is same as that of src1 and operatorChain2.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(operatorConf, (s) -> true);
    final PhysicalSink<String> sink2 = generateSink(idAndConfGenerator.generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);
    final DAG<ExecutionVertex, MISTEdge> query2Plan = new AdjacentListDAG<>();
    GraphUtils.copy(query2, query2Plan);

    // Execute the query 1
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final ImmediateQueryMergingStarter starter = injector.getInstance(ImmediateQueryMergingStarter.class);
    final SrcAndDagMap<String> srcAndDagMap = injector.getInstance(SrcAndDagMap.class);
    final ExecutionDags executionDags = injector.getInstance(ExecutionDags.class);
    final ExecutionPlanDagMap executionPlanDagMap = injector.getInstance(ExecutionPlanDagMap.class);
    final VertexInfoMap vertexInfoMap = injector.getInstance(VertexInfoMap.class);
    starter.start("q1", query1);

    // The query 1 and 2 should be merged and the following dag should be created:
    // src1 -> oc1 -> sink1
    //             -> sink2
    final DAG<ExecutionVertex, MISTEdge> expectedDag = new AdjacentListDAG<>();
    GraphUtils.copy(query1, expectedDag);
    expectedDag.addVertex(sink2);
    expectedDag.addEdge(operatorChain1, sink2, query2.getEdges(operatorChain2).get(sink2));

    // Execute the query 2
    starter.start("q2", query2);

    final DAG<ExecutionVertex, MISTEdge> mergedDag = srcAndDagMap.get(sourceConf);
    Assert.assertEquals(1, srcAndDagMap.size());
    Assert.assertEquals(expectedDag, mergedDag);

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(expectedDag);
    Assert.assertEquals(expectedDags, executionDags.values());

    // Generate events for the merged query and check if the dag is executed correctly
    final String data = "Hello";
    src1.send(data);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(0, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(false, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data), result1);
    Assert.assertEquals(Arrays.asList(data), result2);

    // Check reference count and physical vertex of the execution vertices
    Assert.assertEquals(query1Plan, executionPlanDagMap.get("q1"));
    Assert.assertEquals(query2Plan, executionPlanDagMap.get("q2"));
    Assert.assertEquals(2, vertexInfoMap.get(src1).getRefCount());
    Assert.assertEquals(src1, vertexInfoMap.get(src1).getPhysicalExecutionVertex());
    Assert.assertEquals(2, vertexInfoMap.get(src2).getRefCount());
    Assert.assertEquals(src1, vertexInfoMap.get(src2).getPhysicalExecutionVertex());
    Assert.assertEquals(2, vertexInfoMap.get(operatorChain1).getRefCount());
    Assert.assertEquals(operatorChain1, vertexInfoMap.get(operatorChain1).getPhysicalExecutionVertex());
    Assert.assertEquals(2, vertexInfoMap.get(operatorChain2).getRefCount());
    Assert.assertEquals(operatorChain1, vertexInfoMap.get(operatorChain2).getPhysicalExecutionVertex());
    Assert.assertEquals(1, vertexInfoMap.get(sink1).getRefCount());
    Assert.assertEquals(sink1, vertexInfoMap.get(sink1).getPhysicalExecutionVertex());
    Assert.assertEquals(1, vertexInfoMap.get(sink2).getRefCount());
    Assert.assertEquals(sink2, vertexInfoMap.get(sink2).getPhysicalExecutionVertex());
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
    final TestSource src1 = generateSource(sourceConf);
    final OperatorChain operatorChain1 = generateFilterOperatorChain(
        idAndConfGenerator.generateConf(), (s) -> s.startsWith("Hello"));
    final PhysicalSink<String> sink1 = generateSink(idAndConfGenerator.generateConf(), result1);
    final DAG<ExecutionVertex, MISTEdge> query1 = generateSimpleQuery(src1, operatorChain1, sink1);
    final DAG<ExecutionVertex, MISTEdge> query1Plan = new AdjacentListDAG<>();
    GraphUtils.copy(query1, query1Plan);

    // Create a query 2:
    // src2 -> oc2 -> sink2
    // The configuration of src2 is same as that of src1,
    // but the configuration of oc2 is different from that of oc1.
    final List<String> result2 = new LinkedList<>();
    final TestSource src2 = generateSource(sourceConf);
    final OperatorChain operatorChain2 = generateFilterOperatorChain(
        idAndConfGenerator.generateConf(), (s) -> s.startsWith("World"));
    final PhysicalSink<String> sink2 = generateSink(idAndConfGenerator.generateConf(), result2);
    final DAG<ExecutionVertex, MISTEdge> query2 = generateSimpleQuery(src2, operatorChain2, sink2);
    final DAG<ExecutionVertex, MISTEdge> query2Plan = new AdjacentListDAG<>();
    GraphUtils.copy(query2, query2Plan);

    // Execute the query 1
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();
    jcb.bindImplementation(ExecutionDags.class, MergingExecutionDags.class);
    final Injector injector = Tang.Factory.getTang().newInjector(jcb.build());
    final ImmediateQueryMergingStarter starter = injector.getInstance(ImmediateQueryMergingStarter.class);
    final SrcAndDagMap<String> srcAndDagMap = injector.getInstance(SrcAndDagMap.class);
    final ExecutionDags executionDags = injector.getInstance(ExecutionDags.class);
    final ExecutionPlanDagMap executionPlanDagMap = injector.getInstance(ExecutionPlanDagMap.class);
    final VertexInfoMap vertexInfoMap = injector.getInstance(VertexInfoMap.class);
    starter.start("q1", query1);

    // The query 1 and 2 should be merged and the following dag should be created:
    // src1 -> oc1 -> sink1
    //      -> oc2 -> sink2
    final DAG<ExecutionVertex, MISTEdge> expectedDag = new AdjacentListDAG<>();
    GraphUtils.copy(query1, expectedDag);

    expectedDag.addVertex(operatorChain2);
    expectedDag.addVertex(sink2);
    expectedDag.addEdge(src1, operatorChain2, query2.getEdges(src2).get(operatorChain2));
    expectedDag.addEdge(operatorChain2, sink2, query2.getEdges(operatorChain2).get(sink2));

    // Execute the query 2
    starter.start("q2", query2);

    final DAG<ExecutionVertex, MISTEdge> mergedDag = srcAndDagMap.get(sourceConf);
    Assert.assertEquals(1, srcAndDagMap.size());
    Assert.assertEquals(expectedDag, mergedDag);

    // Check execution dags
    final Collection<DAG<ExecutionVertex, MISTEdge>> expectedDags = new HashSet<>();
    expectedDags.add(expectedDag);
    Assert.assertEquals(expectedDags, executionDags.values());

    // Generate events for the merged query and check if the dag is executed correctly
    final String data1 = "Hello";
    src1.send(data1);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(1, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
    Assert.assertEquals(Arrays.asList(), result2);

    final String data2 = "World";
    src1.send(data2);
    Assert.assertEquals(1, operatorChain1.numberOfEvents());
    Assert.assertEquals(1, operatorChain2.numberOfEvents());
    Assert.assertEquals(true, operatorChain1.processNextEvent());
    Assert.assertEquals(true, operatorChain2.processNextEvent());
    Assert.assertEquals(Arrays.asList(data1), result1);
    Assert.assertEquals(Arrays.asList(data2), result2);

    // Check reference count and physical vertex of the execution vertices
    Assert.assertEquals(query1Plan, executionPlanDagMap.get("q1"));
    Assert.assertEquals(query2Plan, executionPlanDagMap.get("q2"));
    Assert.assertEquals(2, vertexInfoMap.get(src1).getRefCount());
    Assert.assertEquals(src1, vertexInfoMap.get(src1).getPhysicalExecutionVertex());
    Assert.assertEquals(2, vertexInfoMap.get(src2).getRefCount());
    Assert.assertEquals(src1, vertexInfoMap.get(src2).getPhysicalExecutionVertex());
    Assert.assertEquals(1, vertexInfoMap.get(operatorChain1).getRefCount());
    Assert.assertEquals(operatorChain1, vertexInfoMap.get(operatorChain1).getPhysicalExecutionVertex());
    Assert.assertEquals(1, vertexInfoMap.get(operatorChain2).getRefCount());
    Assert.assertEquals(operatorChain2, vertexInfoMap.get(operatorChain2).getPhysicalExecutionVertex());
    Assert.assertEquals(1, vertexInfoMap.get(sink1).getRefCount());
    Assert.assertEquals(sink1, vertexInfoMap.get(sink1).getPhysicalExecutionVertex());
    Assert.assertEquals(1, vertexInfoMap.get(sink2).getRefCount());
    Assert.assertEquals(sink2, vertexInfoMap.get(sink2).getPhysicalExecutionVertex());
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
