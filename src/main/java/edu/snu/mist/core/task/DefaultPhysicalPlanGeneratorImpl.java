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

import edu.snu.mist.api.operators.ApplyStatefulFunction;
import edu.snu.mist.api.sink.parameters.TextSocketSinkParameters;
import edu.snu.mist.api.sources.parameters.*;
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
import java.util.HashMap;
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
  private final NettyTextDataGeneratorFactory nettyDataGeneratorFactory;
  private final KafkaDataGeneratorFactory kafkaDataGeneratorFactory;
  private final NettyTextSinkFactory nettySinkFactory;
  private final String tmpFolderPath;
  private final ScheduledExecutorService scheduler;
  private final ClassLoaderProvider classLoaderProvider;

  @Inject
  private DefaultPhysicalPlanGeneratorImpl(final OperatorIdGenerator operatorIdGenerator,
                                           @Parameter(TempFolderPath.class) final String tmpFolderPath,
                                           final NettyTextDataGeneratorFactory nettyDataGeneratorFactory,
                                           final NettyTextSinkFactory nettySinkFactory,
                                           final KafkaDataGeneratorFactory kafkaDataGeneratorFactory,
                                           final ClassLoaderProvider classLoaderProvider,
                                           final ScheduledExecutorServiceWrapper schedulerWrapper) {
    this.operatorIdGenerator = operatorIdGenerator;
    this.nettyDataGeneratorFactory = nettyDataGeneratorFactory;
    this.nettySinkFactory = nettySinkFactory;
    this.kafkaDataGeneratorFactory = kafkaDataGeneratorFactory;
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
      return nettyDataGeneratorFactory.newDataGenerator(socketHostAddress, Integer.valueOf(socketHostPort));
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /*
   * This private method makes a KafkaDataGenerator from a source configuration.
   */
  private DataGenerator getKafkaDataGenerator(final Map<String, Object> sourceConfString,
                                              final ClassLoader classLoader)
      throws RuntimeException {
    try {
      final String kafkaTopic = sourceConfString.get(KafkaSourceParameters.KAFKA_TOPIC).toString();
      final HashMap<String, Object> kafkaConsumerConfig = deserializeLambda(
          (ByteBuffer) sourceConfString.get(KafkaSourceParameters.KAFKA_CONSUMER_CONFIG), classLoader);
      return kafkaDataGeneratorFactory.newDataGenerator(kafkaTopic, kafkaConsumerConfig);
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /*
   * This private method makes an EventGenerator from a watermark type and configuration.
   */
  private <I, V> EventGenerator<I> getEventGenerator(final WatermarkTypeEnum watermarkType,
                                                     final Map<String, Object> sourceConfString,
                                                     final Map<String, Object> watermarkConfString,
                                                     final ClassLoader classLoader)
      throws RuntimeException {
    try {
      final Function<I, Tuple<V, Long>> timestampExtractionFunc;
      final ByteBuffer serializedExtractionFunc =
          (ByteBuffer) sourceConfString.get(SourceParameters.TIMESTAMP_EXTRACTION_FUNCTION);
      if (serializedExtractionFunc != null) {
        timestampExtractionFunc = deserializeLambda(serializedExtractionFunc, classLoader);
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
          final Predicate<I> isWatermark =
              deserializeLambda((ByteBuffer) watermarkConfString.get(
                  PunctuatedWatermarkParameters.WATERMARK_PREDICATE), classLoader);
          final Function<I, Long> parseTimestamp =
              deserializeLambda((ByteBuffer) watermarkConfString.get(
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
  private Sink getNettyTextSocketSink(final Map<String, Object> sinkConfString)
      throws RuntimeException {
    final String socketHostAddress = sinkConfString.get(TextSocketSinkParameters.SOCKET_HOST_ADDRESS).toString();
    final String socketHostPort = sinkConfString.get(TextSocketSinkParameters.SOCKET_HOST_PORT).toString();
    try {
      return nettySinkFactory.newSink(operatorIdGenerator.generate(),
          socketHostAddress, Integer.valueOf(socketHostPort));
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /*
   * This private method de-serializes byte-serialized lambdas.
   */
  private <T> T deserializeLambda(final ByteBuffer serializedLambda, final ClassLoader classLoader)
      throws IOException, ClassNotFoundException, ClassCastException {
    byte[] serializedByteArray = new byte[serializedLambda.remaining()];
    serializedLambda.get(serializedByteArray);
    final ExternalJarObjectInputStream stream = new ExternalJarObjectInputStream(
        classLoader, serializedByteArray);
    return (T) stream.readObject();
  }

  /*
   * This private method gets instant operator from the serialized instant operator info.
   */
  @SuppressWarnings(value="unchecked")
  private Operator getInstantOperator(final InstantOperatorInfo iOpInfo,
                                      final ClassLoader classLoader)
      throws IllegalArgumentException, IOException, ClassNotFoundException {
    final String operatorId = operatorIdGenerator.generate();
    final List<ByteBuffer> functionList = iOpInfo.getFunctions();
    switch (iOpInfo.getInstantOperatorType()) {
      case APPLY_STATEFUL: {
        final ApplyStatefulFunction applyStatefulFunction =
            deserializeLambda(functionList.get(0), classLoader);
        return new ApplyStatefulOperator<>(operatorId, applyStatefulFunction);
      }
      case FILTER: {
        final Predicate predicate = deserializeLambda(functionList.get(0), classLoader);
        return new FilterOperator<>(operatorId, predicate);
      }
      case FLAT_MAP: {
        final Function flatMapFunc = deserializeLambda(functionList.get(0), classLoader);
        return new FlatMapOperator<>(operatorId, flatMapFunc);
      }
      case MAP: {
        final Function mapFunc = deserializeLambda(functionList.get(0), classLoader);
        return new MapOperator<>(operatorId, mapFunc);
      }
      case REDUCE_BY_KEY: {
        final int keyIndex = iOpInfo.getKeyIndex();
        final BiFunction reduceFunc = deserializeLambda(functionList.get(0), classLoader);
        return new ReduceByKeyOperator<>(operatorId, keyIndex, reduceFunc);
      }
      case REDUCE_BY_KEY_WINDOW: {
        throw new IllegalArgumentException("MISTTask: ReduceByKeyWindowOperator is currently not supported!");
      }
      case APPLY_STATEFUL_WINDOW: {
        final ApplyStatefulFunction applyStatefulFunction =
            deserializeLambda(functionList.get(0), classLoader);
        return new ApplyStatefulWindowOperator<>(operatorId, applyStatefulFunction);
      }
      case AGGREGATE_WINDOW: {
        final Function aggregateFunc = deserializeLambda(functionList.get(0), classLoader);
        return new AggregateWindowOperator<>(operatorId, aggregateFunc);
      }
      case UNION: {
        return new UnionOperator(operatorId);
      }
      default: {
        throw new IllegalArgumentException("MISTTask: Invalid InstantOperatorType detected!");
      }
    }
  }

  /*
   * This private method gets window operator from the serialized window operator info.
   */
  private Operator getWindowOperator(final WindowOperatorInfo wOpInfo,
                                     final ClassLoader classLoader)
      throws IllegalArgumentException, IOException, ClassNotFoundException {
    final String operatorId = operatorIdGenerator.generate();
    final Operator operator;
    switch (wOpInfo.getWindowOperatorType()) {
      case TIME:
        operator = new TimeWindowOperator<>(
            operatorId, wOpInfo.getWindowSize(), wOpInfo.getWindowInterval());
        break;
      case COUNT:
        operator = new CountWindowOperator<>(
            operatorId, wOpInfo.getWindowSize(), wOpInfo.getWindowInterval());
        break;
      case SESSION:
        operator = new SessionWindowOperator<>(
            operatorId, wOpInfo.getWindowInterval());
        break;
      case JOIN:
        final List<ByteBuffer> functionList = wOpInfo.getFunctions();
        final BiPredicate joinPred = deserializeLambda(functionList.get(0), classLoader);
        operator = new JoinOperator<>(operatorId, joinPred);
        break;
      default:
        throw new IllegalArgumentException("MISTTask: Invalid window operator type " +
            wOpInfo.getWindowOperatorType().toString());
    }
    return operator;
  }

  @Override
  public DAG<PhysicalVertex, Direction> generate(
      final Tuple<String, LogicalPlan> queryIdAndLogicalPlan)
      throws IllegalArgumentException, IOException, ClassNotFoundException {
    final String queryId = queryIdAndLogicalPlan.getKey();
    final LogicalPlan logicalPlan = queryIdAndLogicalPlan.getValue();
    final List<PhysicalVertex> deserializedVertices = new ArrayList<>();
    final DAG<PhysicalVertex, Direction> dag = new AdjacentListDAG<>();

    // Get a class loader
    final ClassLoader classLoader = classLoaderProvider.newInstance(logicalPlan.getJarFilePaths());

    // Deserialize vertices
    for (final AvroVertexChain avroVertexChain : logicalPlan.getAvroVertices()) {
      switch (avroVertexChain.getAvroVertexChainType()) {
        case SOURCE: {
          final Vertex vertex = avroVertexChain.getVertexChain().get(0);
          final SourceInfo sourceInfo = (SourceInfo) vertex.getAttributes();
          final StringIdentifierFactory identifierFactory = new StringIdentifierFactory();
          final Map<String, Object> sourceStringConf = sourceInfo.getSourceConfiguration();
          final Map<String, Object> watermarkStringConf = sourceInfo.getWatermarkConfiguration();
          final EventGenerator eventGenerator =
              getEventGenerator(sourceInfo.getWatermarkType(), sourceStringConf, watermarkStringConf, classLoader);
          final Source source;

          switch (sourceInfo.getSourceType()) {
            case TEXT_SOCKET_SOURCE: {
              final DataGenerator<String> textDataGenerator =
                  getNettyTextDataGenerator(sourceStringConf);
              source = new SourceImpl<>(identifierFactory.getNewInstance(operatorIdGenerator.generate()),
                  textDataGenerator, eventGenerator);
              break;
            }
            case KAFKA_SOURCE: {
              final DataGenerator kafkaDataGenerator =
                  getKafkaDataGenerator(sourceStringConf, classLoader);
              source = new SourceImpl<>(identifierFactory.getNewInstance(operatorIdGenerator.generate()),
                  kafkaDataGenerator, eventGenerator);
              break;
            }
            default: {
              throw new IllegalArgumentException("MISTTask: Invalid source generator detected in LogicalPlan!");
            }
          }

          deserializedVertices.add(source);
          dag.addVertex(source);
          break;
        }
        case OPERATOR_CHAIN: {
          final PartitionedQuery partitionedQuery = new DefaultPartitionedQuery();
          deserializedVertices.add(partitionedQuery);
          for (final Vertex vertex : avroVertexChain.getVertexChain()) {
            switch (vertex.getVertexType()) {
              case INSTANT_OPERATOR: {
                final InstantOperatorInfo iOpInfo = (InstantOperatorInfo) vertex.getAttributes();
                final Operator operator = getInstantOperator(iOpInfo, classLoader);
                partitionedQuery.insertToTail(operator);
                break;
              }
              case WINDOW_OPERATOR: {
                final WindowOperatorInfo wOpInfo = (WindowOperatorInfo) vertex.getAttributes();
                final Operator operator = getWindowOperator(wOpInfo, classLoader);
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
              final Sink textSocketSink = getNettyTextSocketSink(sinkConf);
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
      dag.addEdge(deserializedSrcVertex, deserializedDstVertex, edge.getDirection());
    }
    return dag;
  }
}