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

import edu.snu.mist.api.functions.MISTBiFunction;
import edu.snu.mist.api.functions.MISTFunction;
import edu.snu.mist.api.functions.MISTPredicate;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.formats.avro.*;
import edu.snu.mist.task.common.parameters.SocketServerIp;
import edu.snu.mist.task.common.parameters.SocketServerPort;
import edu.snu.mist.task.operators.Operator;
import edu.snu.mist.task.operators.parameters.KeyIndex;
import edu.snu.mist.task.operators.parameters.OperatorId;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sinks.parameters.SinkId;
import edu.snu.mist.task.sources.SourceGenerator;
import edu.snu.mist.task.sources.parameters.SourceId;
import edu.snu.mist.utils.AvroSerializer;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;

/**
 * This class deserializes physical plan and create instances of vertices (operators and sinks).
 * As mist keeps sources in memory because of receiving input,
 * It doesn't deserialize and create instances of sources.
 */
final class PhysicalPlanDeserializer {

  private static final Logger LOG = Logger.getLogger(PhysicalPlanDeserializer.class.getName());

  private PhysicalPlanDeserializer() {
  }

  /*
   * This private method makes a TextSocketStreamGenerator from a source configuration.
   */
  private static SourceGenerator getTextSocketStreamGenerator(final String queryId,
                                                              final String sourceId,
                                                              final Map<CharSequence, Object> sourceConf,
                                                              final Class clazz)
      throws IllegalArgumentException, InjectionException {
    final Map<String, Object> sourceConfString = new HashMap<>();
    for (final CharSequence charSeqKey : sourceConf.keySet()) {
      sourceConfString.put(charSeqKey.toString(), sourceConf.get(charSeqKey));
    }
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final String socketHostAddress = sourceConfString.get(TextSocketSourceParameters.SOCKET_HOST_ADDRESS).toString();
    final String socketHostPort = sourceConfString.get(TextSocketSourceParameters.SOCKET_HOST_PORT).toString();
    cb.bindNamedParameter(SocketServerIp.class, socketHostAddress);
    cb.bindNamedParameter(SocketServerPort.class, socketHostPort);
    cb.bindNamedParameter(QueryId.class, queryId);
    cb.bindNamedParameter(SourceId.class, sourceId);
    return (SourceGenerator)Tang.Factory.getTang().newInjector(cb.build()).getInstance(clazz);
  }

  /*
   * This private method makes a TextSocketSink from a sink configuration.
   */
  private static Sink getTextSocketSink(final String queryId,
                                        final String sinkId,
                                        final Map<CharSequence, Object> sinkConf,
                                        final Class clazz)
      throws IllegalArgumentException, InjectionException {
    final Map<String, Object> sinkConfString = new HashMap<>();
    for (final CharSequence charSeqKey : sinkConf.keySet()) {
      sinkConfString.put(charSeqKey.toString(), sinkConf.get(charSeqKey));
    }
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    final String socketHostAddress = sinkConfString.get(TextSocketSinkParameters.SOCKET_HOST_ADDRESS).toString();
    final String socketHostPort = sinkConfString.get(TextSocketSinkParameters.SOCKET_HOST_PORT).toString();
    cb.bindNamedParameter(SocketServerIp.class, socketHostAddress);
    cb.bindNamedParameter(SocketServerPort.class, socketHostPort);
    cb.bindNamedParameter(QueryId.class, queryId);
    cb.bindNamedParameter(SinkId.class, sinkId);
    return (Sink)Tang.Factory.getTang().newInjector(cb.build()).getInstance(clazz);
  }

  /*
   * This private method de-serializes byte-serialized lambdas
   */
  private static Object deserializeLambda(final ByteBuffer serializedLambda) {
    byte[] serializedByteArray = new byte[serializedLambda.remaining()];
    serializedLambda.get(serializedByteArray);
    return SerializationUtils.deserialize(serializedByteArray);
  }

  /*
   * This private method gets instant operator from the serialized instant operator info.
   */
  private static Operator getInstantOperator(final String queryId,
                                             final String operatorId,
                                             final InstantOperatorInfo iOpInfo,
                                             final Class clazz)
      throws IllegalArgumentException, InjectionException {
    final JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
    cb.bindNamedParameter(QueryId.class, queryId);
    cb.bindNamedParameter(OperatorId.class, operatorId);
    final List<ByteBuffer> functionList = iOpInfo.getFunctions();
    switch (iOpInfo.getInstantOperatorType()) {
      case APPLY_STATEFUL: {
        throw new IllegalArgumentException("MISTTask: ApplyStatefulOperator is currently not supported!");
      }
      case FILTER: {
        final MISTPredicate predicate = (MISTPredicate) deserializeLambda(functionList.get(0));
        final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
        injector.bindVolatileInstance(MISTPredicate.class, predicate);
        return (Operator)injector.getInstance(clazz);
      }
      case FLAT_MAP: {
        final MISTFunction flatMapFunc = (MISTFunction) deserializeLambda(functionList.get(0));
        final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
        injector.bindVolatileInstance(MISTFunction.class, flatMapFunc);
        return (Operator)injector.getInstance(clazz);
      }
      case MAP: {
        final MISTFunction mapFunc = (MISTFunction) deserializeLambda(functionList.get(0));
        final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
        injector.bindVolatileInstance(MISTFunction.class, mapFunc);
        return (Operator)injector.getInstance(clazz);
      }
      case REDUCE_BY_KEY: {
        cb.bindNamedParameter(KeyIndex.class, iOpInfo.getKeyIndex().toString());
        final MISTBiFunction reduceFunc = (MISTBiFunction) deserializeLambda(functionList.get(0));
        final Injector injector = Tang.Factory.getTang().newInjector(cb.build());
        injector.bindVolatileInstance(MISTBiFunction.class, reduceFunc);
        return (Operator)injector.getInstance(clazz);
      }
      case REDUCE_BY_KEY_WINDOW: {
        throw new IllegalArgumentException("MISTTask: ReduceByKeyWindowOperator is currently not supported!");
      }
      default: {
        throw new IllegalArgumentException("MISTTask: Invalid InstantOperatorType detected!");
      }
    }
  }

  /**
   * Deserialize except sources.
   * As mist keeps sources in memory because of receiving input,
   * we don't have to deserialize and create instances of sources.
   * @param serializedPlan serializedPlan
   * @return physical plan
   * @throws IllegalArgumentException
   * @throws InjectionException
   */
  public static PhysicalPlan<Operator> deserialize(final String serializedPlan)
      throws IllegalArgumentException, InjectionException {
    final AvroPhysicalPlan avroPhysicalPlan = AvroSerializer.avroFromString(serializedPlan,
        AvroPhysicalPlan.getClassSchema(), AvroPhysicalPlan.class);
    final String queryId = avroPhysicalPlan.getQueryId().toString();
    final Map<String, Object> deserializedVertices = new HashMap<>();
    final Map<String, Set<Operator>> sourceMap = new HashMap<>();
    final DAG<Operator> operatorDAG = new AdjacentListDAG<>();
    final Map<Operator, Set<Sink>> sinkMap = new HashMap<>();

    // Deserialize vertices
    for (final Map.Entry<CharSequence, PhysicalVertex> entry : avroPhysicalPlan.getPhysicalVertices().entrySet()) {
      final AvroVertex vertex = entry.getValue().getLogicalInfo();
      try {
        final Class clazz = Class.forName(entry.getValue().getPhysicalVertexClass().toString());
        final String vertexId = entry.getKey().toString();
        switch (vertex.getVertexType()) {
          case SOURCE: {
            final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
            switch (sourceInfo.getSourceType()) {
              case TEXT_SOCKET_SOURCE: {
                final SourceGenerator textSocketStreamGenerator
                    = getTextSocketStreamGenerator(queryId, entry.getKey().toString(),
                    sourceInfo.getSourceConfiguration(), clazz);
                deserializedVertices.put(vertexId, textSocketStreamGenerator);
                break;
              }
              case REEF_NETWORK_SOURCE: {
                throw new IllegalArgumentException("MISTTask: REEF_NETWORK_SOURCE is currently not supported!");
              }
              default: {
                throw new IllegalArgumentException("MISTTask: Invalid source generator detected in LogicalPlan!");
              }
            }
            break;
          }
          case INSTANT_OPERATOR: {
            final InstantOperatorInfo iOpInfo = (InstantOperatorInfo) vertex.getAttributes();
            final Operator operator = getInstantOperator(queryId, vertexId, iOpInfo, clazz);
            deserializedVertices.put(vertexId, operator);
            operatorDAG.addVertex(operator);
            break;
          }
          case WINDOW_OPERATOR: {
            throw new IllegalArgumentException("MISTTask: WindowOperator is currently not supported!");
          }
          case SINK: {
            final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
            switch (sinkInfo.getSinkType()) {
              case TEXT_SOCKET_SINK: {
                final Sink textSocketSink = getTextSocketSink(queryId,
                    entry.getKey().toString(), sinkInfo.getSinkConfiguration(), clazz);
                deserializedVertices.put(vertexId, textSocketSink);
                break;
              }
              case REEF_NETWORK_SINK: {
                throw new IllegalArgumentException("MISTTask: REEF_NETWORK_SINK is currently not supported!");
              }
              default: {
                throw new IllegalArgumentException("MISTTask: Invalid sink detected in LogicalPlan!");
              }
            }
            break;
          }
          default: {
            throw new IllegalArgumentException("MISTTask: Invalid vertex detected in LogicalPlan!");
          }
        }
      } catch (final ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
    // Add edge info to physical plan
    for (final Map.Entry<CharSequence, PhysicalVertex> entry : avroPhysicalPlan.getPhysicalVertices().entrySet()) {
      switch (entry.getValue().getLogicalInfo().getVertexType()) {
        case SOURCE:
          final Set<Operator> operatorsConnectedToSrc = new HashSet<>();
          sourceMap.put(entry.getKey().toString(), operatorsConnectedToSrc);

          for (final CharSequence dstVertexId : entry.getValue().getEdges()) {
            final Operator op = (Operator) deserializedVertices.get(dstVertexId.toString());
            operatorsConnectedToSrc.add(op);
          }
          break;
        case INSTANT_OPERATOR:
          final Object srcElement = deserializedVertices.get(entry.getKey().toString());
          for (final CharSequence dstVertexId : entry.getValue().getEdges()) {
            final Object nextOp = deserializedVertices.get(dstVertexId.toString());
            if (nextOp instanceof Sink) {
              if (!sinkMap.containsKey(nextOp)) {
                sinkMap.put((Operator) srcElement, new HashSet<>());
              }
              sinkMap.get(srcElement).add((Sink) nextOp);
            } else {
              operatorDAG.addEdge((Operator) srcElement, (Operator) nextOp);
            }
          }
          break;
        case WINDOW_OPERATOR:
          throw new IllegalStateException("MISTTask: WindowOperator is currently not supported but MIST didn't " +
              "catch it in advance!");
        case SINK:
          break;
        default:
          throw new IllegalArgumentException("Unsupported type");
      }
    }
    // source is null because mist already holds the sources.
    return new DefaultPhysicalPlanImpl<>(null, sourceMap, operatorDAG, sinkMap);
  }
}