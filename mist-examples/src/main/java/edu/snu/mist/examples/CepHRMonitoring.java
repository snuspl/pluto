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
import edu.snu.mist.client.MISTQueryBuilder;
import edu.snu.mist.client.cep.*;
import edu.snu.mist.common.functions.MISTFunction;
import edu.snu.mist.common.functions.MISTPredicate;
import edu.snu.mist.common.cep.CepEventContiguity;
import edu.snu.mist.common.cep.CepEventPattern;
import edu.snu.mist.examples.parameters.NettySourceAddress;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.CommandLine;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

/**
 * Example client which submits a cep query.
 * The input event consists of string(D or P) and integer(heart rate) with split character comma(,).
 * Doctor's input event would be set the heart rate, and monitoring the patient's heart rate.
 * When the patient's heart rate is monotone increasing and over the doctor's setting heart rate,
 * the alert signal would be sent to the sink server.
 * Cep event pattern: D --(Non-deterministic relaxed contiguity)--P(2+, strict inner contiguity)
 */
public final class CepHRMonitoring {
  public static APIQueryControlResult submitQuery(final Configuration configuration)
      throws IOException, InjectionException, URISyntaxException {
    final String sourceSocket =
        Tang.Factory.getTang().newInjector(configuration).getNamedInstance(NettySourceAddress.class);
    final String[] source = sourceSocket.split(":");
    final String sourceHostname = source[0];
    final int sourcePort = Integer.parseInt(source[1]);

    final CepInput<CepHRClass> input = new CepInput.TextSocketBuilder<CepHRClass>()
        .setSocketAddress(sourceHostname)
        .setSocketPort(sourcePort)
        .setClassGenFunc(new CepHRClassGenFunc())
        .build();

    final CepSink sink = new CepSink.TextSocketBuilder()
        .setSocketAddress(MISTExampleUtils.SINK_HOSTNAME)
        .setSocketPort(MISTExampleUtils.SINK_PORT)
        .build();

    final MISTPredicate<CepHRClass> conditionD = s -> s.getName().equals("D");
    final MISTPredicate<CepHRClass> conditionP = s -> s.getName().equals("P");

    final CepEventPattern<CepHRClass> eventD = new CepEventPattern.Builder<CepHRClass>()
        .setName("doctor")
        .setCondition(conditionD)
        .setContiguity(CepEventContiguity.NON_DETERMINISTIC_RELAXED)
        .setClass(CepHRClass.class)
        .build();

    final CepEventPattern<CepHRClass> eventP = new CepEventPattern.Builder<CepHRClass>()
        .setName("patient")
        .setCondition(conditionP)
        .setContiguity(CepEventContiguity.NON_DETERMINISTIC_RELAXED)
        .setNOrMore(2)
        .setInnerContiguity(CepEventContiguity.STRICT)
        .setClass(CepHRClass.class)
        .build();

    final MISTCepQuery<CepHRClass> cepQuery = new MISTCepQuery.Builder<CepHRClass>("demo-group", "user1")
        .input(input)
        .setEventPatternSequence(eventD, eventP)
        .setQualifier(new CepHRQualifier())
        .within(3600000)
        .setAction(new CepAction.Builder()
            .setActionType(CepActionType.TEXT_WRITE)
            .setCepSink(sink)
            .setParams("Alert!")
            .build())
        .build();

    final MISTQueryBuilder queryBuilder = CepUtils.translate(cepQuery);
    return MISTExampleUtils.submit(queryBuilder, configuration);

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
        .processCommandLine(args);

    if (commandLine == null) {  // Option '?' was entered and processCommandLine printed the help.
      return;
    }

    Thread sinkServer = new Thread(MISTExampleUtils.getSinkServer());
    sinkServer.start();

    final APIQueryControlResult result = submitQuery(jcb.build());

    System.out.println("Query submission result: " + result.getQueryId());
  }

  private static final class CepHRClass {
    private final String name;
    private final int rate;

    private CepHRClass(final String name, final int rate) {
      this.name = name;
      this.rate = rate;
    }

    private String getName() {
      return name;
    }

    private int getRate() {
      return rate;
    }
  }

  private static final class CepHRClassGenFunc implements MISTFunction<String, CepHRClass> {

    @Override
    public CepHRClass apply(final String s) {
      final String[] split = s.split(" ");
      return new CepHRClass(split[0], Integer.parseInt(split[1]));
    }

    private CepHRClassGenFunc() {
    }
  }

  private static class CepHRQualifier implements CepQualifier<CepHRClass> {

    @Override
    public boolean test(final Map<String, List<CepHRClass>> stringListMap) {
      final List<CepHRClass> patientEventList = stringListMap.get("patient");
      final CepHRClass doctorEvent = stringListMap.get("doctor").get(0);
      if (doctorEvent.getRate() >= patientEventList.get(patientEventList.size() - 1).getRate()) {
        return false;
      }
      for (int i = 0; i < patientEventList.size() - 1; i++) {
        if (patientEventList.get(i).getRate() >= patientEventList.get(i + 1).getRate()) {
          return false;
        }
      }
      return true;
    }
  }

  private CepHRMonitoring() {
  }
}
