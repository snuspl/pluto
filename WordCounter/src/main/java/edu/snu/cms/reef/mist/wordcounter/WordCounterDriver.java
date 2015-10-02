/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package edu.snu.cms.reef.mist.wordcounter;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.io.network.naming.NameResolverConfiguration;
import org.apache.reef.io.network.naming.NameServer;
import org.apache.reef.tang.*;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import javax.inject.Inject;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Driver code for the WordCounter Application
 */
@Unit
public final class WordCounterDriver {

  private static final Logger LOG = Logger.getLogger(WordCounterDriver.class.getName());
  private final EvaluatorRequestor requestor;
  private final NameServer nameServer;
  private final String senderName, receiverName;
  private final AtomicInteger submittedContext;
  private final AtomicInteger submittedTask;
  private ActiveContext senderContext;
  private final String driverHostAddress;

  /**
   * Job driver constructor - instantiated via TANG.
   *
   * @param requestor evaluator requestor object used to create new evaluator containers.
   */
  @Inject
  private WordCounterDriver(final EvaluatorRequestor requestor) throws UnknownHostException, InjectionException {
    this.requestor = requestor;
    LOG.log(Level.FINE, "Instantiated 'WordCounterDriver'");
    Injector injector = Tang.Factory.getTang().newInjector();
    this.nameServer = injector.getInstance(NameServer.class);
    this.submittedContext = new AtomicInteger();
    this.submittedContext.set(0);
    this.submittedTask = new AtomicInteger();
    this.submittedTask.set(0);
    this.senderName = "sender";
    this.receiverName = "receiver";
    this.driverHostAddress = Inet4Address.getLocalHost().getHostAddress();
  }

  /**
   * Handles the StartTime event: Request as single Evaluator.
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      WordCounterDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(2)
          .setMemory(64)
          .setNumberOfCores(1)
          .build());
      LOG.log(Level.INFO, "Requested Evaluator.");
    }
  }

  /**
   * Handles AllocatedEvaluator: Submit the WordCounterTask.
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.FINE, "Evaluator allocated");
      final Configuration contextConf;
      if (submittedContext.compareAndSet(0, 1)) {
        contextConf = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "context_0")
            .build();
      } else {
        contextConf = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "context_1")
            .build();
      }
      allocatedEvaluator.submitContext(contextConf);
    }
  }

  public final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext context) {
      if (submittedTask.compareAndSet(0, 1)) {
        // keep context for the sender task
        senderContext = context;
      } else {
        final Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "receiver_task")
            .set(TaskConfiguration.TASK, WordCounterTask.class)
            .build();
        final Configuration netConf = NameResolverConfiguration.CONF
            .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, driverHostAddress)
            .set(NameResolverConfiguration.NAME_SERVICE_PORT, WordCounterDriver.this.nameServer.getPort())
            .build();
        final JavaConfigurationBuilder taskConfBuilder =
            Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
        taskConfBuilder.bindNamedParameter(WordCounterTask.ReceiverName.class, receiverName);
        final Configuration taskConf = taskConfBuilder.build();
        context.submitTask(taskConf);
      }
    }
  }

  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(final RunningTask task) {
      if (task.getId().equals("receiver_task")) {
        // receiver task is ready, submit sender task
        final Configuration partialTaskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "sender_task")
            .set(TaskConfiguration.TASK, WordGeneratorTask.class)
            .build();
        final Configuration netConf = NameResolverConfiguration.CONF
            .set(NameResolverConfiguration.NAME_SERVER_HOSTNAME, driverHostAddress)
            .set(NameResolverConfiguration.NAME_SERVICE_PORT, WordCounterDriver.this.nameServer.getPort())
            .build();
        final JavaConfigurationBuilder taskConfBuilder =
            Tang.Factory.getTang().newConfigurationBuilder(partialTaskConf, netConf);
        taskConfBuilder.bindNamedParameter(WordGeneratorTask.ReceiverName.class, receiverName);
        taskConfBuilder.bindNamedParameter(WordGeneratorTask.SenderName.class, senderName);
        final Configuration taskConf = taskConfBuilder.build();
        senderContext.submitTask(taskConf);
      }
    }
  }
}
