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
package edu.snu.mist.common.configurations;

public final class ConfKeys {

  private ConfKeys() {

  }

  public enum SourceConf {
    TIMESTAMP_EXTRACT_FUNC,
    SOURCE_TYPE
  }

  public enum KafkaSourceConf {
    KAFKA_TOPIC,
    KAFKA_CONSUMER_CONFIG
  }

  public enum NettySourceConf {
    SOURCE_ADDR,
    SOURCE_PORT
  }

  public enum MQTTSourceConf {
    MQTT_SRC_BROKER_URI,
    MQTT_SRC_TOPIC
  }

  public enum OperatorConf {
    OP_TYPE,
    UDF_STRING,
  }

  public enum StateTransitionOperator {
    INITIAL_STATE,
    FINAL_STATE,
    STATE_TABLE
  }

  public enum CepOperator {
    CEP_EVENT,
    WINDOW_TIME
  }

  public enum ReduceByKeyOperator {
    KEY_INDEX,
    MIST_BI_FUNC
  }

  public enum WindowOperator {
    WINDOW_SIZE,
    WINDOW_INTERVAL
  }

  public enum ConditionalBranchOperator {
    UDF_LIST_STRING
  }

  public enum SinkConf {
    SINK_TYPE,
  }

  public enum NettySink {
    SINK_ADDRESS,
    SINK_PORT,
  }

  public enum MqttSink {
    MQTT_SINK_BROKER_URI,
    MQTT_SINK_TOPIC,
  }

  public enum Watermark {
    PERIODIC_WATERMARK_PERIOD,
    PERIODIC_WATERMARK_DELAY,
    EVENT_GENERATOR,
    TIMESTAMP_PARSE_OBJECT,
    WATERMARK_PREDICATE
  }
}
