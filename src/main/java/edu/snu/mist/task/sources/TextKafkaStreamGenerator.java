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

package edu.snu.mist.task.sources;

import edu.snu.mist.common.parameters.QueryId;
import edu.snu.mist.task.common.OutputEmitter;
import edu.snu.mist.task.sources.parameters.*;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.Identifier;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class TextKafkaStreamGenerator implements SourceGenerator<String>{
  /**
   * An output emitter.
   */
  protected OutputEmitter<String> outputEmitter;

  /**
   * A flag for close.
   */
  protected final AtomicBoolean closed;

  /**
   * A flag for start.
   */
  protected final AtomicBoolean started;

  /**
   * An executor service running this source generator.
   * TODO[MIST-152]: Threads of SourceGenerator should be managed judiciously.
   */
  private final ExecutorService executorService;

  /**
   * Time to sleep when fetched data is null.
   */
  private final long sleepTime;

  /**
   * Identifier of SourceGenerator.
   */
  protected final Identifier identifier;

  /**
   * Identifier of Query.
   */
  protected final Identifier queryId;

  /**
   * The Kafka consumer.
   */
  private final ConsumerConnector consumer;

  /**
   * The number of threads to assign for each topic.
   */
  private final int NUM_THREADS = 1;

  /**
   * The KafkaStreams for a certain topic.
   */
  List<KafkaStream<byte[], byte[]>> streams;

  /**
   * In order for KafkaStreams to work, the Zookeeper and Kafka server must be running.
   * Note that the Consumer connects to the Zookeeper server, while the Producer connects to the Kafka Server.
   */
  @Inject
  private TextKafkaStreamGenerator(@Parameter (ZkSourceAddress.class) final String zkAddress,
                                   @Parameter (ZkSourcePort.class)final int zkPort,
                                   @Parameter (KafkaTopicName.class)final String topic,
                                   @Parameter (DataFetchSleepTime.class)final long sleepTime,
                                   @Parameter (SourceId.class)final String sourceId,
                                   @Parameter (QueryId.class)final String queryId,
                                   final StringIdentifierFactory identifierFactory) {
    this.executorService = Executors.newSingleThreadExecutor();
    this.closed = new AtomicBoolean(false);
    this.started = new AtomicBoolean(false);
    this.sleepTime = sleepTime;
    this.queryId = identifierFactory.getNewInstance(queryId);
    this.identifier = identifierFactory.getNewInstance(sourceId);

    //Property setup for kafka consumer.
    Properties props = new Properties();
    props.put("group.id", "group1");
    props.put("zookeeper.connect", zkAddress+":"+zkPort);
    ConsumerConfig consumerConfig = new ConsumerConfig(props);

    //Creating the kafka consumer
    consumer = Consumer.createJavaConsumerConnector(consumerConfig);
    Map<String, Integer> topicCountMap = new HashMap<>();

    //We assume that only one thread is dedicated to the consumer group. Thus, one thread reads from the single topic.
    //TODO [MIST-205] : One kafka source to read inputs from multiple sources.
    topicCountMap.put(topic, NUM_THREADS); //Assign a certain number of threads to the topic.
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    streams = consumerMap.get(topic);
  }

  @Override
  public void start(){
    if (started.compareAndSet(false, true)){
      for (final KafkaStream<byte[], byte[]> stream: streams){
        executorService.execute(new Runnable() {
          @Override
          public void run() {
            while (!closed.get()) {
              try {
                String input = null;
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while(it.hasNext()){
                  input = new String(it.next().message());
                  if (outputEmitter == null) {
                    throw new RuntimeException("OutputEmitter should be set in " + BaseSourceGenerator.class.getName());
                  }
                  if (input.equals("")) { //"" signifies that the input is empty.
                    Thread.sleep(sleepTime);
                  } else {
                    outputEmitter.emit(input);
                  }
                }
              } catch (final InterruptedException e){
                e.printStackTrace();
              }
            }
          }
        });
      }
    }
  }

  /**
   * Releases IO resources.
   * This method is called just once.
   * @throws Exception
   */
  public void releaseResources() throws Exception{
    consumer.shutdown();
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      releaseResources();
      executorService.shutdown();
    }
  }

  @Override
  public Identifier getIdentifier() {
    return identifier;
  }

  @Override
  public Identifier getQueryIdentifier() {
    return queryId;
  }

  @Override
  public void setOutputEmitter(final OutputEmitter<String> emitter) {
    this.outputEmitter = emitter;
  }
}
