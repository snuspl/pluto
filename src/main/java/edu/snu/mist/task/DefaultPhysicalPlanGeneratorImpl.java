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
package edu.snu.mist.task;

import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.formats.avro.*;
import edu.snu.mist.task.common.parameters.SocketServerIp;
import edu.snu.mist.task.common.parameters.SocketServerPort;
import edu.snu.mist.task.operators.*;
import edu.snu.mist.task.operators.parameters.OperatorId;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sinks.TextSocketSink;
import edu.snu.mist.task.sources.SourceGenerator;
import edu.snu.mist.task.sources.TextSocketStreamGenerator;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A default implementation of PhysicalPlanGenerator.
 */
final class DefaultPhysicalPlanGeneratorImpl implements PhysicalPlanGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultPhysicalPlanGeneratorImpl.class);

  private final OperatorIdGenerator operatorIdGenerator;

  @Inject
  private DefaultPhysicalPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator) {
    this.operatorIdGenerator = operatorIdGenerator;
  }

  /*
   * This private method makes a TextSocketStreamGenerator from a source configuration.
   */
  private TextSocketStreamGenerator getTextSocketStreamGenerator(final Map<CharSequence, Object> sourceConf)
    throws IllegalArgumentException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final String socketHostAddress = (String) sourceConf.get(TextSocketSourceParameters.SOCKET_HOST_ADDRESS);
    final String socketHostPort = sourceConf.get(TextSocketSourceParameters.SOCKET_HOST_PORT).toString();
    cb.bindNamedParameter(SocketServerIp.class, socketHostAddress);
    cb.bindNamedParameter(SocketServerPort.class, socketHostPort);
    return Tang.Factory.getTang().newInjector(cb.build()).getInstance(TextSocketStreamGenerator.class);
  }

  /*
   * This private method makes a TextSocketSink from a sink configuration.
   */
  private TextSocketSink getTextSocketSink(final Map<CharSequence, Object> sinkConf)
    throws IllegalArgumentException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final String socketHostAddress = (String) sinkConf.get(TextSocketSinkParameters.SOCKET_HOST_ADDRESS);
    final String socketHostPort = sinkConf.get(TextSocketSinkParameters.SOCKET_HOST_PORT).toString();
    cb.bindNamedParameter(SocketServerIp.class, socketHostAddress);
    cb.bindNamedParameter(SocketServerPort.class, socketHostPort);
    return Tang.Factory.getTang().newInjector(cb.build()).getInstance(TextSocketSink.class);
  }

  /*
   * This private method de-serializes byte-serialized lambdas
   */
  private Object deserializeLambda(final ByteBuffer serializedLambda) {
    byte[] serializedByteArray = new byte[serializedLambda.remaining()];
    serializedLambda.get(serializedByteArray);
    return SerializationUtils.deserialize(serializedByteArray);
  }

  /*
   * This private method gets instant operator from the serialized instant operator info.
   */
  private Operator getInstantOperator(final String queryId, final InstantOperatorInfo iOpInfo)
      throws IllegalArgumentException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(QueryId.class, queryId);
    cb.bindNamedParameter(OperatorId.class, operatorIdGenerator.generate());
    final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
    final List<ByteBuffer> functionList = iOpInfo.getFunctions();
    switch(iOpInfo.getInstantOperatorType()) {
      case APPLY_STATEFUL:
        throw new IllegalArgumentException("MISTTask: ApplyStatefulOperator is currently not supported!");
      case FILTER:
        final Predicate predicate = (Predicate) deserializeLambda(functionList.get(0));
        injector.bindVolatileInstance(Predicate.class, predicate);
        return injector.getInstance(FilterOperator.class);
      case FLAT_MAP:
        final Function flatMapFunc = (Function) deserializeLambda(functionList.get(0));
        injector.bindVolatileInstance(Function.class, flatMapFunc);
        return injector.getInstance(FlatMapOperator.class);
      case MAP:
        final Function mapFunc = (Function) deserializeLambda(functionList.get(0));
        injector.bindVolatileInstance(Function.class, mapFunc);
        return injector.getInstance(MapOperator.class);
      case REDUCE_BY_KEY:
        final BiFunction reduceFunc = (BiFunction) deserializeLambda(functionList.get(0));
        injector.bindVolatileInstance(BiFunction.class, reduceFunc);
        return injector.getInstance(ReduceByKeyOperator.class);
      case REDUCE_BY_KEY_WINDOW:
        throw new IllegalArgumentException("MISTTask: ReduceByKeyWindowOperator is currently not supported!");
      default:
        throw new IllegalArgumentException("MISTTask: Invalid InstantOperatorType detected!");
    }
  }

  @Override
  public PhysicalPlan<Operator> generate(final Tuple<String, LogicalPlan> queryIdAndLogicalPlan)
      throws IllegalArgumentException, InjectionException {
    final String queryId = queryIdAndLogicalPlan.getKey();
    final LogicalPlan logicalPlan = queryIdAndLogicalPlan.getValue();
    final List<Object> deserializedVertices = new ArrayList<>();
    final Map<SourceGenerator, Set<Operator>> sourceMap = new HashMap<>();
    final DAG<Operator> operators = new AdjacentListDAG<>();
    final Map<Operator, Set<Sink>> sinkMap = new HashMap<>();
    // Deserialize vertices
    for (final Vertex vertex : logicalPlan.getVertices()) {
      switch(vertex.getVertexType()) {
        case SOURCE:
          final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
          if (sourceInfo.getSourceType() == SourceTypeEnum.TEXT_SOCKET_SOURCE) {
            final TextSocketStreamGenerator textSocketStreamGenerator;
            textSocketStreamGenerator = getTextSocketStreamGenerator(sourceInfo.getSourceConfiguration());
            deserializedVertices.add(textSocketStreamGenerator);
          } else if (sourceInfo.getSourceType() == SourceTypeEnum.REEF_NETWORK_SOURCE) {
            throw new IllegalArgumentException("MISTTask: REEF_NETWORK_SOURCE is currently not supported!");
          } else {
            throw new IllegalArgumentException("MISTTask: Invalid source generator detected in LogicalPlan!");
          }
          break;
        case INSTANT_OPERATOR:
          final InstantOperatorInfo iOpInfo = (InstantOperatorInfo) vertex.getAttributes();
          final Operator operator = getInstantOperator(queryId, iOpInfo);
          deserializedVertices.add(operator);
          operators.addVertex(operator);
          break;
        case WINDOW_OPERATOR:
          throw new IllegalArgumentException("MISTTask: WindowOperator is currently not supported!");
        case SINK:
          final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
          if (sinkInfo.getSinkType() == SinkTypeEnum.TEXT_SOCKET_SINK) {
            final TextSocketSink textSocketSink;
            textSocketSink = getTextSocketSink(sinkInfo.getSinkConfiguration());
            deserializedVertices.add(textSocketSink);
          } else if (sinkInfo.getSinkType() == SinkTypeEnum.REEF_NETWORK_SINK) {
            throw new IllegalArgumentException("MISTTask: REEF_NETWORK_SINK is currently not supported!");
          } else {
            throw new IllegalArgumentException("MISTTask: Invalid sink detected in LogicalPlan!");
          }
          break;
        default:
          throw new IllegalArgumentException("MISTTask: Invalid vertex detected in LogicalPlan!");
      }
    }
    // Add edge info to physical plan
    for (final Edge edge : logicalPlan.getEdges()) {
      final int fromIndex = edge.getFrom();
      final Object deserializedFromVertex = deserializedVertices.get(fromIndex);
      final int toIndex = edge.getTo();
      final Object deserializedToVertex = deserializedVertices.get(toIndex);
      switch(logicalPlan.getVertices().get(fromIndex).getVertexType()) {
        case SOURCE:
          if (!sourceMap.containsKey(deserializedFromVertex)) {
            sourceMap.put((SourceGenerator) deserializedFromVertex, new HashSet<>());
          }
          sourceMap.get(deserializedFromVertex).add((Operator) deserializedToVertex);
          break;
        case INSTANT_OPERATOR:
          switch (logicalPlan.getVertices().get(toIndex).getVertexType()) {
            case INSTANT_OPERATOR:
              operators.addEdge((Operator) deserializedFromVertex, (Operator) deserializedToVertex);
              break;
            case WINDOW_OPERATOR:
              throw new IllegalStateException("MISTTask: WindowOperator is currently not supported but MIST didn't " +
                  "catch it in advance!");
            case SINK:
              if (!sinkMap.containsKey(deserializedFromVertex)) {
                sinkMap.put((Operator) deserializedFromVertex, new HashSet<>());
              }
              sinkMap.get(deserializedFromVertex).add((Sink) deserializedToVertex);
              break;
            default:
              // ToVertex type is SourceGenerator, but it's illegal!
              throw new IllegalArgumentException("MISTTask: Invalid edge detected! SourceGenerator cannot have" +
                  " ingoing edges!");
          }
          break;
        case WINDOW_OPERATOR:
          throw new IllegalStateException("MISTTask: WindowOperator is currently not supported but MIST didn't catch" +
              " it in advance!");
        default:
          // FromVertex type is guaranteed to be Sink! However, Sink cannot have outgoing edges!
          throw new IllegalArgumentException("MISTTask: Invalid edge detected! Sink cannot have outgoing edges!");
      }
    }
    return new DefaultPhysicalPlanImpl<>(sourceMap, operators, sinkMap);
  }
}