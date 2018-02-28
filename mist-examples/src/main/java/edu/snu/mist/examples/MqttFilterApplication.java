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

package edu.snu.mist.examples;

import edu.snu.mist.client.APIQueryControlResult;
import edu.snu.mist.client.MISTDefaultExecutionEnvironmentImpl;
import edu.snu.mist.client.MISTExecutionEnvironment;
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.datastreams.configurations.MQTTSourceConfiguration;
import edu.snu.mist.client.datastreams.configurations.SourceConfiguration;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.examples.parameters.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Submit a query that filters prefix strings.
 * We can submit multiple different queries that filter different strings from this class.
 * To do this, we first submit the mist-example jar file to MIST.
 *
 * Required parameters:
 * -bu: broker address (default value: localhost:1883)
 * -source_topic: mqtt source topic
 * -sink_topic: mqtt sink topic
 * -filtered_string: filtered string prefix
 * -app_id: application identifier
 */
public final class MqttFilterApplication {

  public static APIQueryControlResult submitQuery(final Configuration configuration)
      throws IOException, InjectionException, URISyntaxException {
    final Injector injector = Tang.Factory.getTang().newInjector(configuration);
    final String brokerURI = injector.getNamedInstance(TestMQTTBrokerURI.class);
    final String sourceTopic = injector.getNamedInstance(MqttSourceTopic.class);
    final String sinkTopic = injector.getNamedInstance(MqttSinkTopic.class);
    final String filteredString = injector.getNamedInstance(FilteredString.class);
    final String appId = injector.getNamedInstance(ApplicationIdentifier.class);

    final SourceConfiguration mqttSourceConf = new MQTTSourceConfiguration.MQTTSourceConfigurationBuilder()
        .setBrokerURI(brokerURI)
        .setTopic(sourceTopic)
        .build();

    final MISTQueryBuilder queryBuilder = new MISTQueryBuilder();
    queryBuilder.setApplicationId(appId)
        .mqttStream(mqttSourceConf)
        .map(new MqttMessageToStringFunc())
        .filter(new FilterFunc(filteredString))
        .map(new MapFunc(filteredString))
        .mqttOutput(brokerURI, sinkTopic);

    final String[] driverSocket =
        Tang.Factory.getTang().newInjector(configuration).getNamedInstance(DriverAddress.class).split(":");
    final String driverHostname = driverSocket[0];
    final int driverPort = Integer.parseInt(driverSocket[1]);

    try (final MISTExecutionEnvironment executionEnvironment =
        new MISTDefaultExecutionEnvironmentImpl(driverHostname, driverPort)) {
      return executionEnvironment.submitQuery(queryBuilder.build());
    } catch (final Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  /**
   * Set the environment(Hostname and port of driver, source, and sink) and submit a query.
   * @param args command line parameters
   * @throws Exception
   */
  public static void main(final String[] args) throws Exception {
    final JavaConfigurationBuilder jcb = Tang.Factory.getTang().newConfigurationBuilder();

    final CommandLine commandLine = MISTExampleUtils.getDefaultCommandLine(jcb)
        .registerShortNameOfClass(NettySourceAddress.class) // Additional parameter
        .registerShortNameOfClass(MqttSourceTopic.class)
        .registerShortNameOfClass(MqttSinkTopic.class)
        .registerShortNameOfClass(FilteredString.class)
        .registerShortNameOfClass(ApplicationIdentifier.class)
        .processCommandLine(args);

    if (commandLine == null) {  // Option '?' was entered and processCommandLine printed the help.
      return;
    }

    final APIQueryControlResult result = submitQuery(jcb.build());
    System.out.println("Query submission result: " + result.getQueryId());

  }

  /**
   * Must not be instantiated.
   */
  private MqttFilterApplication() {
  }

  @NamedParameter(short_name = "filtered_string", default_value = "HelloMIST:")
  public static final class FilteredString implements Name<String> {
    // do nothing
  }

  public static final class MqttMessageToStringFunc implements MISTFunction<MqttMessage, String> {

    @Override
    public String apply(final MqttMessage mqttMessage) {
      return new String(mqttMessage.getPayload());
    }

    @Override
    public boolean equals(final Object o) {
      return o instanceof MqttMessageToStringFunc;
    }

    @Override
    public int hashCode() {
      return 1;
    }
  }

  public static final class FilterFunc implements MISTPredicate<String> {

    private final String filteredStr;

    public FilterFunc(final String filteredStr) {
      this.filteredStr = filteredStr;
    }

    @Override
    public boolean test(final String s) {
      return s.startsWith(filteredStr);
    }

    // WE must override equals and hashcode to allow query merging
    // Two queries will be merged in MIST when the UDFs equal
    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final FilterFunc that = (FilterFunc) o;

      if (filteredStr != null ? !filteredStr.equals(that.filteredStr) : that.filteredStr != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return filteredStr != null ? filteredStr.hashCode() : 0;
    }
  }

  public static final class MapFunc implements MISTFunction<String, MqttMessage> {
    private final String prefix;
    public MapFunc(final String prefix) {
      this.prefix = prefix;
    }

    @Override
    public MqttMessage apply(final String s) {
      return new MqttMessage(s.substring(prefix.length()).trim().getBytes());
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      final MapFunc mapFunc = (MapFunc) o;

      if (prefix != null ? !prefix.equals(mapFunc.prefix) : mapFunc.prefix != null) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode() {
      return prefix != null ? prefix.hashCode() : 0;
    }
  }
}