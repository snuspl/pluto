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

import edu.snu.mist.common.operators.Operator;
import edu.snu.mist.common.parameters.OperatorId;
import edu.snu.mist.common.shared.KafkaSharedResource;
import edu.snu.mist.common.shared.NettySharedResource;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.implementation.java.ClassHierarchyImpl;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a helper class that creates physical objects (sources, operators, sinks)
 * from serialized configurations.
 */
final class PhysicalObjectGenerator implements AutoCloseable {

  /**
   * Avro configuration serializer that serializes/deserializes objects.
   */
  private final AvroConfigurationSerializer avroSerializer;

  /**
   * Scheduled executor for event generators.
   */
  private final ScheduledExecutorService scheduler;

  /**
   * Time unit for watermarks.
   */
  private final TimeUnit watermarkTimeUnit = TimeUnit.MILLISECONDS;

  /**
   * Kafka shared resources.
   */
  private final KafkaSharedResource kafkaSharedResource;

  /**
   * Netty shared resouces.
   */
  private final NettySharedResource nettySharedResource;

  @Inject
  private PhysicalObjectGenerator(final AvroConfigurationSerializer avroSerializer,
                                  final ScheduledExecutorServiceWrapper schedulerWrapper,
                                  final KafkaSharedResource kafkaSharedResource,
                                  final NettySharedResource nettySharedResource) {
    this.avroSerializer = avroSerializer;
    this.scheduler = schedulerWrapper.getScheduler();
    this.kafkaSharedResource = kafkaSharedResource;
    this.nettySharedResource = nettySharedResource;
  }

  /**
   * Get an injector from the serialized configuration with the external class loader.
   * @param confString serialized configuration
   * @param classLoader external class loader
   * @param urls urls for jar paths
   * @return injector
   */
  private Injector newDefaultInjector(final String confString,
                                      final ClassLoader classLoader,
                                      final URL[] urls) throws IOException {
    final Configuration conf = avroSerializer.fromString(confString,
        new ClassHierarchyImpl(urls));
    final Injector injector = Tang.Factory.getTang().newInjector(conf);
    injector.bindVolatileInstance(ClassLoader.class, classLoader);
    return injector;
  }

  /**
   * Get a new event generator.
   * @param confString serialized configuration
   * @param classLoader external class loader
   * @param urls urls for jar paths
   * @param <T> event type
   * @return event generator
   */
  @SuppressWarnings("unchecked")
  public <T> EventGenerator<T> newEventGenerator(
      final String confString,
      final ClassLoader classLoader,
      final URL[] urls) throws IOException, InjectionException {
    final Injector injector = newDefaultInjector(confString, classLoader, urls);
    injector.bindVolatileInstance(TimeUnit.class, watermarkTimeUnit);
    injector.bindVolatileInstance(ScheduledExecutorService.class, scheduler);
    return injector.getInstance(EventGenerator.class);
  }

  /**
   * Get a new data generator.
   * @param confString serialized configuration
   * @param classLoader external class loader
   * @param urls urls for jar paths
   * @param <T> data type
   * @return data generator
   */
  @SuppressWarnings("unchecked")
  public <T> DataGenerator<T> newDataGenerator(
      final String confString,
      final ClassLoader classLoader,
      final URL[] urls) throws IOException, InjectionException {
    final Injector injector = newDefaultInjector(confString, classLoader, urls);
    // for netty
    injector.bindVolatileInstance(NettySharedResource.class, nettySharedResource);
    // for kafka
    injector.bindVolatileInstance(KafkaSharedResource.class, kafkaSharedResource);
    return injector.getInstance(DataGenerator.class);
  }

  /**
   * Get a new operator.
   * @param operatorId operator id
   * @param confString serialized configuration
   * @param classLoader external class loader
   * @param urls urls for jar paths
   * @return new operator
   */
  @SuppressWarnings("unchecked")
  public Operator newOperator(
      final String operatorId,
      final String confString,
      final ClassLoader classLoader,
      final URL[] urls) throws IOException, InjectionException {
    final Injector injector = newDefaultInjector(confString, classLoader, urls);
    injector.bindVolatileParameter(OperatorId.class, operatorId);
    return injector.getInstance(Operator.class);
  }

  /**
   * Get a new sink.
   * @param sinkId sink id
   * @param confString serialized configuration
   * @param classLoader external class loader
   * @param urls urls for jar paths
   * @return new sink
   */
  @SuppressWarnings("unchecked")
  public <T> Sink<T> newSink(
      final String sinkId,
      final String confString,
      final ClassLoader classLoader,
      final URL[] urls) throws IOException, InjectionException {
    final Injector injector = newDefaultInjector(confString, classLoader, urls);
    injector.bindVolatileParameter(OperatorId.class, sinkId);
    // for netty
    injector.bindVolatileInstance(NettySharedResource.class, nettySharedResource);
    // for kafka
    return injector.getInstance(Sink.class);
  }

  @Override
  public void close() throws Exception {
    kafkaSharedResource.close();
    nettySharedResource.close();
  }
}
