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
package edu.snu.mist.core.task;

import edu.snu.mist.api.StreamType;
import edu.snu.mist.api.operators.ApplyStatefulFunction;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.parameters.PeriodicWatermarkParameters;
import edu.snu.mist.api.sources.parameters.PunctuatedWatermarkParameters;
import edu.snu.mist.api.sources.parameters.TextSocketSourceParameters;
import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.ExternalJarObjectInputStream;
import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.core.task.common.PhysicalVertex;
import edu.snu.mist.core.task.operators.*;
import edu.snu.mist.core.task.sinks.NettyTextSinkFactory;
import edu.snu.mist.core.task.sinks.Sink;
import edu.snu.mist.core.task.sources.*;
import edu.snu.mist.formats.avro.*;
import org.apache.reef.io.Tuple;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
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
  private final ClassLoaderProvider classLoaderProvider;

  @Inject
  private DefaultPhysicalPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator,
                                           @Parameter(TempFolderPath.class) final String tmpFolderPath,
                                           final NettyTextDataGeneratorFactory dataGeneratorFactory,
                                           final NettyTextSinkFactory sinkFactory,
                                           final ClassLoaderProvider classLoaderProvider,
                                           final ScheduledExecutorServiceWrapper schedulerWrapper) {
    this.operatorIdGenerator = operatorIdGenerator;
    this.dataGeneratorFactory = dataGeneratorFactory;
    this.sinkFactory = sinkFactory;
    this.tmpFolderPath = tmpFolderPath;
    this.classLoaderProvider = classLoaderProvider;
    this.scheduler= schedulerWrapper.getScheduler();
  }

  /*
   * This private method makes a NettyTextDataGenerator from a source configuration.
   */
  private DataGenerator<String> getNettyTextDataGenerator(final Map<String, Object> sourceConfString)
      throws RuntimeException {
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
   * This private method makes an EventGenerator from a watermark type and configuration.
   */
  private EventGenerator<String> getEventGenerator(final WatermarkTypeEnum watermarkType,
                                                   final Map<String, Object> sourceConfString,
                                                   final Map<String, Object> watermarkConfString,
                                                   final ClassLoader classLoader)
      throws RuntimeException {
    try {
      final Function<String, Tuple<String, Long>> timestampExtractionFunc;
      final ByteBuffer serializedExtractionFunc =
          (ByteBuffer) sourceConfString.get(TextSocketSourceParameters.TIMESTAMP_EXTRACTION_FUNCTION);
      if (serializedExtractionFunc != null) {
        timestampExtractionFunc = (Function) deserializeLambda(serializedExtractionFunc, classLoader);
      } else {
        timestampExtractionFunc = null;
      }

      switch (watermarkType) {
        case PERIODIC:
          final long period = Long.parseLong(watermarkConfString.get(PeriodicWatermarkParameters.PERIOD).toString());
          final long expectedDelay =
              Long.parseLong(watermarkConfString.get(PeriodicWatermarkParameters.EXPECTED_DELAY).toString());
          return new PeriodicEventGenerator<>(
              timestampExtractionFunc, period, expectedDelay, TimeUnit.MILLISECONDS, scheduler);
        case PUNCTUATED:
          final Predicate<String> isWatermark =
              (Predicate) deserializeLambda((ByteBuffer) watermarkConfString.get(
                  PunctuatedWatermarkParameters.WATERMARK_PREDICATE), classLoader);
          final Function<String, Long> parseTimestamp =
              (Function) deserializeLambda((ByteBuffer) watermarkConfString.get(
                  PunctuatedWatermarkParameters.PARSING_TIMESTAMP_FROM_WATERMARK), classLoader);
          return new PunctuatedEventGenerator<>(timestampExtractionFunc, isWatermark, parseTimestamp);
        default:
          throw new IllegalArgumentException("MISTTask: Invalid WatermarkTypeEnum is detected!");
      }
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /*
   * This private method makes a NettyTextSocketSink from a sink configuration.
   */
  private Sink getNettyTextSocketSink(final String queryId, final Map<String, Object> sinkConfString)
      throws RuntimeException {
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
    final ExternalJarObjectInputStream stream = new ExternalJarObjectInputStream(
          classLoader, serializedByteArray);
      return stream.readObject();
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
        final ApplyStatefulFunction applyStatefulFunction =
            (ApplyStatefulFunction) deserializeLambda(functionList.get(0), classLoader);
        return new ApplyStatefulOperator<>(queryId, operatorId, applyStatefulFunction);
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
      case APPLY_STATEFUL_WINDOW: {
        final ApplyStatefulFunction applyStatefulFunction =
            (ApplyStatefulFunction) deserializeLambda(functionList.get(0), classLoader);
        return new ApplyStatefulWindowOperator<>(queryId, operatorId, applyStatefulFunction);
      }
      case AGGREGATE_WINDOW: {
        final Function aggregateFunc = (Function) deserializeLambda(functionList.get(0), classLoader);
        return new AggregateWindowOperator<>(queryId, operatorId, aggregateFunc);
      }
      case UNION: {
        return new UnionOperator(queryId, operatorId);
      }
      default: {
        throw new IllegalArgumentException("MISTTask: Invalid InstantOperatorType detected!");
      }
    }
  }

  /*
   * This private method gets window operator from the serialized window operator info.
   */
  private Operator getWindowOperator(final String queryId, final WindowOperatorInfo wOpInfo,
                                     final ClassLoader classLoader)
      throws IllegalArgumentException, IOException, ClassNotFoundException {
    final String operatorId = operatorIdGenerator.generate();
    final Operator operator;
    switch (wOpInfo.getWindowOperatorType()) {
      case TIME:
        operator = new TimeWindowOperator<>(
            queryId, operatorId, wOpInfo.getWindowSize(), wOpInfo.getWindowInterval());
        break;
      case COUNT:
        operator = new CountWindowOperator<>(
            queryId, operatorId, wOpInfo.getWindowSize(), wOpInfo.getWindowInterval());
        break;
      case SESSION:
        operator = new SessionWindowOperator<>(
            queryId, operatorId, wOpInfo.getWindowInterval());
        break;
      case JOIN:
        final List<ByteBuffer> functionList = wOpInfo.getFunctions();
        final BiPredicate joinPred = (BiPredicate) deserializeLambda(functionList.get(0), classLoader);
        operator = new JoinOperator<>(queryId, operatorId, joinPred);
        break;
      default:
        throw new IllegalArgumentException("MISTTask: Invalid window operator type " +
            wOpInfo.getWindowOperatorType().toString());
    }
    return operator;
  }

  @Override
  public DAG<PhysicalVertex, StreamType.Direction> generate(
      final Tuple<String, LogicalPlan> queryIdAndLogicalPlan)
      throws IllegalArgumentException, IOException, ClassNotFoundException {
    final String queryId = queryIdAndLogicalPlan.getKey();
    final LogicalPlan logicalPlan = queryIdAndLogicalPlan.getValue();
    final List<PhysicalVertex> deserializedVertices = new ArrayList<>();
    final DAG<PhysicalVertex, StreamType.Direction> dag = new AdjacentListDAG<>();

    // Get a class loader
    final ClassLoader classLoader = classLoaderProvider.newInstance(logicalPlan.getJarFilePaths());

    // Deserialize vertices
    for (final AvroVertexChain avroVertexChain : logicalPlan.getAvroVertices()) {
      switch (avroVertexChain.getAvroVertexChainType()) {
        case SOURCE: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
          switch (sourceInfo.getSourceType()) {
            case TEXT_SOCKET_SOURCE: {
              final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
              final Map<String, Object> sourceConf = sourceInfo.getSourceConfiguration();
              final Map<String, Object> watermarkConf = sourceInfo.getWatermarkConfiguration();
              final DataGenerator<String> textDataGenerator =
                  getNettyTextDataGenerator(sourceConf);
              final EventGenerator<String> eventGenerator =
                  getEventGenerator(sourceInfo.getWatermarkType(), sourceConf,
                      watermarkConf, classLoader);
              final Source<String> textSocketSource = new SourceImpl<>(identifierFactory.getNewInstance(queryId),
                  identifierFactory.getNewInstance(operatorIdGenerator.generate()),
                  textDataGenerator, eventGenerator);
              deserializedVertices.add(textSocketSource);
              dag.addVertex(textSocketSource);
              break;
            }
            default: {
              throw new IllegalArgumentException("MISTTask: Invalid source generator detected in LogicalPlan!");
            }
          }
          break;
        }
        case OPERATOR_CHAIN: {
          final PartitionedQuery partitionedQuery = new DefaultPartitionedQuery();
          deserializedVertices.add(partitionedQuery);
          for (final Vertex vertex : avroVertexChain.getVertexChain()) {
            switch (vertex.getVertexType()) {
              case INSTANT_OPERATOR: {
                final InstantOperatorInfo iOpInfo = (InstantOperatorInfo) vertex.getAttributes();
                final Operator operator = getInstantOperator(queryId, iOpInfo, classLoader);
                partitionedQuery.insertToTail(operator);
                break;
              }
              case WINDOW_OPERATOR: {
                final WindowOperatorInfo wOpInfo = (WindowOperatorInfo) vertex.getAttributes();
                final Operator operator = getWindowOperator(queryId, wOpInfo, classLoader);
                partitionedQuery.insertToTail(operator);
                break;
              }
              default: {
                throw new IllegalArgumentException("MISTTask: Invalid operator type" + vertex.getVertexType());
              }
            }
          }
          dag.addVertex(partitionedQuery);
          break;
        }
        case SINK: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final SinkInfo sinkInfo = (SinkInfo) vertex.getAttributes();
          switch (sinkInfo.getSinkType()) {
            case TEXT_SOCKET_SINK: {
              final Map<String, Object> sinkConf = sinkInfo.getSinkConfiguration();
              final Sink textSocketSink = getNettyTextSocketSink(queryId, sinkConf);
              deserializedVertices.add(textSocketSink);
              dag.addVertex(textSocketSink);
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
      final PhysicalVertex deserializedSrcVertex = deserializedVertices.get(srcIndex);
      final int dstIndex = edge.getTo();
      final PhysicalVertex deserializedDstVertex = deserializedVertices.get(dstIndex);
      StreamType.Direction direction;
      if (edge.getIsLeft()) {
        direction = StreamType.Direction.LEFT;
      } else {
        direction = StreamType.Direction.RIGHT;
      }
      dag.addEdge(deserializedSrcVertex, deserializedDstVertex, direction);
    }
    return dag;
  }
}