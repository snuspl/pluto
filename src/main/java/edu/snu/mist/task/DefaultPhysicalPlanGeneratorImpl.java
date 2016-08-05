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

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.ExternalJarObjectInputStream;
import edu.snu.mist.driver.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.*;
import edu.snu.mist.task.operators.*;
import edu.snu.mist.task.sinks.NettyTextSinkFactory;
import edu.snu.mist.task.sinks.Sink;
import edu.snu.mist.task.sources.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * A default implementation of PhysicalPlanGenerator.
 */
final class DefaultPhysicalPlanGeneratorImpl implements PhysicalPlanGenerator {

  private static final Logger LOG = Logger.getLogger(DefaultPhysicalPlanGeneratorImpl.class.getName());

  private final OperatorIdGenerator operatorIdGenerator;
  private final NettyTextDataGeneratorFactory dataGeneratorFactory;
  private final NettyTextSinkFactory sinkFactory;
  private final String tmpFolderPath;
  private final ScheduledExecutorService scheduler;

  @Inject
  private DefaultPhysicalPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator,
                                           @Parameter(TempFolderPath.class) final String tmpFolderPath,
                                           final NettyTextDataGeneratorFactory dataGeneratorFactory,
                                           final NettyTextSinkFactory sinkFactory,
                                           final ScheduledExecutorServiceWrapper schedulerWrapper) {
    this.operatorIdGenerator = operatorIdGenerator;
    this.dataGeneratorFactory = dataGeneratorFactory;
    this.sinkFactory = sinkFactory;
    this.tmpFolderPath = tmpFolderPath;
    this.scheduler= schedulerWrapper.getScheduler();
  }

  /*
   * This private method makes a NettyTextDataGenerator from a source configuration.
   */
  private DataGenerator<String> getNettyTextDataGenerator(final Map<CharSequence, Object> sourceConf)
      throws IllegalArgumentException {
    final Map<String, Object> sourceConfString = new HashMap<>();
    for (final Map.Entry<CharSequence, Object> entry : sourceConf.entrySet()) {
      sourceConfString.put(entry.getKey().toString(), entry.getValue());
    }
    final String socketHostAddress = sourceConfString.get(TextSocketSourceParameters.SOCKET_HOST_ADDRESS).toString();
    final String socketHostPort = sourceConfString.get(TextSocketSourceParameters.SOCKET_HOST_PORT).toString();
    try {
      return dataGeneratorFactory.newDataGenerator(socketHostAddress, Integer.valueOf(socketHostPort));
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /*
   * This private method makes a NettyTextSocketSink from a sink configuration.
   */
  private Sink getNettyTextSocketSink(final String queryId, final Map<CharSequence, Object> sinkConf)
      throws IllegalArgumentException {
    final Map<String, Object> sinkConfString = new HashMap<>();
    for (final Map.Entry<CharSequence, Object> entry : sinkConf.entrySet()) {
      sinkConfString.put(entry.getKey().toString(), entry.getValue());
    }
    final String socketHostAddress = sinkConfString.get(TextSocketSinkParameters.SOCKET_HOST_ADDRESS).toString();
    final String socketHostPort = sinkConfString.get(TextSocketSinkParameters.SOCKET_HOST_PORT).toString();
    try {
      return sinkFactory.newSink(queryId, operatorIdGenerator.generate(),
          socketHostAddress, Integer.valueOf(socketHostPort));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /*
   * This private method de-serializes byte-serialized lambdas
   */
  private Object deserializeLambda(final ByteBuffer serializedLambda, final ClassLoader classLoader)
      throws IOException, ClassNotFoundException {
    byte[] serializedByteArray = new byte[serializedLambda.remaining()];
    serializedLambda.get(serializedByteArray);
    if (classLoader == null) {
      return SerializationUtils.deserialize(serializedByteArray);
    } else {
      ExternalJarObjectInputStream stream = new ExternalJarObjectInputStream(
          classLoader, serializedByteArray);
      return stream.readObject();
    }
  }

  /*
   * This private method gets instant operator from the serialized instant operator info.
   */
  @SuppressWarnings(value="unchecked")
  private Operator getInstantOperator(final String queryId, final InstantOperatorInfo iOpInfo,
                                      final ClassLoader classLoader)
      throws IllegalArgumentException, IOException, ClassNotFoundException {
    final String operatorId = operatorIdGenerator.generate();
    final List<ByteBuffer> functionList = iOpInfo.getFunctions();
    switch (iOpInfo.getInstantOperatorType()) {
      case APPLY_STATEFUL: {
        throw new IllegalArgumentException("MISTTask: ApplyStatefulOperator is currently not supported!");
      }
      case FILTER: {
        final Predicate predicate = (Predicate) deserializeLambda(functionList.get(0), classLoader);
        return new FilterOperator<>(queryId, operatorId, predicate);
      }
      case FLAT_MAP: {
        final Function flatMapFunc = (Function) deserializeLambda(functionList.get(0), classLoader);
        return new FlatMapOperator<>(queryId, operatorId, flatMapFunc);
      }
      case MAP: {
        final Function mapFunc = (Function) deserializeLambda(functionList.get(0), classLoader);
        return new MapOperator<>(queryId, operatorId, mapFunc);
      }
      case REDUCE_BY_KEY: {
        final int keyIndex = iOpInfo.getKeyIndex();
        final BiFunction reduceFunc = (BiFunction) deserializeLambda(functionList.get(0), classLoader);
        return new ReduceByKeyOperator<>(queryId, operatorId, keyIndex, reduceFunc);
      }
      case REDUCE_BY_KEY_WINDOW: {
        throw new IllegalArgumentException("MISTTask: ReduceByKeyWindowOperator is currently not supported!");
      }
      case UNION: {
        return new UnionOperator(queryId, operatorId);
      }
      default: {
        throw new IllegalArgumentException("MISTTask: Invalid InstantOperatorType detected!");
      }
    }
  }

  @Override
  public PhysicalPlan<Operator, StreamType.Direction> generate(
      final Tuple<String, LogicalPlan> queryIdAndLogicalPlan)
      throws IllegalArgumentException, IOException, ClassNotFoundException {
    final String queryId = queryIdAndLogicalPlan.getKey();
    final LogicalPlan logicalPlan = queryIdAndLogicalPlan.getValue();
    final List<Object> deserializedVertices = new ArrayList<>();
    final Map<Source, Map<Operator, StreamType.Direction>> sourceMap = new HashMap<>();
    final DAG<Operator, StreamType.Direction> operators = new AdjacentListDAG<>();
    final Map<Operator, Set<Sink>> sinkMap = new HashMap<>();
    final Path jarFilePath = Paths.get(tmpFolderPath, String.format("%s.jar", queryId));

    // Deserialize Jar
    final ClassLoader userQueryClassLoader;
    if (!queryIdAndLogicalPlan.getValue().getIsJarSerialized()) {
      userQueryClassLoader = null;
    } else {
      final ByteBuffer byteBufferJar = queryIdAndLogicalPlan.getValue().getJar();
      final byte[] byteArrayJar = new byte[byteBufferJar.remaining()];
      byteBufferJar.get(byteArrayJar);
      FileUtils.writeByteArrayToFile(jarFilePath.toFile(), byteArrayJar);
      userQueryClassLoader = new URLClassLoader(new URL[]{jarFilePath.toUri().toURL()});
    }

    // Deserialize vertices
    for (final Vertex vertex : logicalPlan.getVertices()) {
      switch (vertex.getVertexType()) {
        case SOURCE: {
          final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
          switch (sourceInfo.getSourceType()) {
            case TEXT_SOCKET_SOURCE: {
              final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
              final DataGenerator<String> textDataGenerator =
                  getNettyTextDataGenerator(sourceInfo.getSourceConfiguration());
              // TODO: [MIST-285] get the text socket's watermark information from user.
              final EventGenerator<String> eventGenerator =
                  new PeriodicEventGenerator<>(null, 100L, 0L, TimeUnit.MILLISECONDS, scheduler);
              final Source<String> textSocketSource = new SourceImpl<>(identifierFactory.getNewInstance(queryId),
                  identifierFactory.getNewInstance(operatorIdGenerator.generate()),
                  textDataGenerator, eventGenerator);
              deserializedVertices.add(textSocketSource);
              break;
            }
            default: {
              throw new IllegalArgumentException("MISTTask: Invalid source generator detected in LogicalPlan!");
            }
          }
          break;
        }
        case INSTANT_OPERATOR: {
          final InstantOperatorInfo iOpInfo = (InstantOperatorInfo) vertex.getAttributes();
          final Operator operator = getInstantOperator(queryId, iOpInfo, userQueryClassLoader);
          deserializedVertices.add(operator);
          operators.addVertex(operator);
          break;
        }
        case WINDOW_OPERATOR: {
          throw new IllegalArgumentException("MISTTask: WindowOperator is currently not supported!");
        }
        case SINK: {
          final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
          switch (sinkInfo.getSinkType()) {
            case TEXT_SOCKET_SINK: {
              final Sink textSocketSink = getNettyTextSocketSink(queryId, sinkInfo.getSinkConfiguration());
              deserializedVertices.add(textSocketSink);
              break;
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
    }
    // Add edge info to physical plan
    for (final Edge edge : logicalPlan.getEdges()) {
      final int srcIndex = edge.getFrom();
      final Object deserializedSrcVertex = deserializedVertices.get(srcIndex);
      final int dstIndex = edge.getTo();
      final Object deserializedDstVertex = deserializedVertices.get(dstIndex);
      StreamType.Direction direction;
      if (edge.getIsLeft()) {
        direction = StreamType.Direction.LEFT;
      } else {
        direction = StreamType.Direction.RIGHT;
      }

      switch (logicalPlan.getVertices().get(srcIndex).getVertexType()) {
        case SOURCE: {
          if (!sourceMap.containsKey(deserializedSrcVertex)) {
            sourceMap.put((Source) deserializedSrcVertex, new HashMap<>());
          }
          sourceMap.get(deserializedSrcVertex).put((Operator) deserializedDstVertex, direction);
          break;
        }
        case INSTANT_OPERATOR: {
          switch (logicalPlan.getVertices().get(dstIndex).getVertexType()) {
            case INSTANT_OPERATOR: {
              operators.addEdge((Operator) deserializedSrcVertex, (Operator) deserializedDstVertex, direction);
              break;
            }
            case WINDOW_OPERATOR: {
              throw new IllegalStateException("MISTTask: WindowOperator is currently not supported but MIST didn't " +
                  "catch it in advance!");
            }
            case SINK: {
              if (!sinkMap.containsKey(deserializedSrcVertex)) {
                sinkMap.put((Operator) deserializedSrcVertex, new HashSet<>());
              }
              sinkMap.get(deserializedSrcVertex).add((Sink) deserializedDstVertex);
              break;
            }
            default: {
              // ToVertex type is Source, but it's illegal!
              throw new IllegalArgumentException("MISTTask: Invalid edge detected! Source cannot have" +
                  " ingoing edges!");
            }
          }
          break;
        }
        case WINDOW_OPERATOR: {
          throw new IllegalStateException("MISTTask: WindowOperator is currently not supported but MIST didn't catch" +
              " it in advance!");
        }
        default: {
          // FromVertex type is guaranteed to be Sink! However, Sink cannot have outgoing edges!
          throw new IllegalArgumentException("MISTTask: Invalid edge detected! Sink cannot have outgoing edges!");
        }
      }
    }
    return new DefaultPhysicalPlanImpl<>(sourceMap, operators, sinkMap);
  }
}