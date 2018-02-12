/*
 * Copyright (C) 2017 Seoul National University
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

import edu.snu.mist.common.rpc.*;
import edu.snu.mist.core.parameters.ClientToTaskServerPortNum;
import edu.snu.mist.core.parameters.MasterToTaskServerPortNum;
import edu.snu.mist.formats.avro.ClientToTaskMessage;
import edu.snu.mist.formats.avro.MasterToTaskMessage;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A runtime engine running mist queries.
 * The submitted queries are handled by QueryManager.
 */
@Unit
public final class MistTask implements Task {

  private static final Logger LOG = Logger.getLogger(MistTask.class.getName());

  private final Tang tang = Tang.Factory.getTang();

  /**
   * A count down latch for sleeping and terminating this task.
   */
  private final CountDownLatch countDownLatch;

  /**
   * The Avro RPC server used to get messages from servers.
   */
  private final Server masterToTaskServer;

  /**
   * The Avro RPC server used to get messages from clients.
   */
  private final Server clientToTaskServer;

  /**
   * The query manager.
   */
  private final QueryManager queryManager;

  /**
   * Default constructor of MistTask.
   * @param queryManager the query manager
   * @param masterToTaskPortNum internal port number used for internal Avro RPC
   * @param clientToTaskPortNum external port number used for internal Avro RPC
   * @throws InjectionException
   */
  @Inject
  private MistTask(final QueryManager queryManager,
                   @Parameter(MasterToTaskServerPortNum.class) final int masterToTaskPortNum,
                   @Parameter(ClientToTaskServerPortNum.class) final int clientToTaskPortNum) throws
      InjectionException {
    this.countDownLatch = new CountDownLatch(1);

    final JavaConfigurationBuilder masterToTaskServerConfBuilder = tang.newConfigurationBuilder();
    masterToTaskServerConfBuilder.bindImplementation(MasterToTaskMessage.class, DefaultMasterToTaskMessageImpl.class);
    masterToTaskServerConfBuilder.bindConstructor(SpecificResponder.class, MasterToTaskSpecificResponderWrapper.class);
    masterToTaskServerConfBuilder.bindConstructor(Server.class, AvroRPCNettyServerWrapper.class);
    masterToTaskServerConfBuilder.bindNamedParameter(RPCServerPort.class, String.valueOf(masterToTaskPortNum));
    this.masterToTaskServer
        = tang.newInjector(masterToTaskServerConfBuilder.build()).getInstance(Server.class);

    final JavaConfigurationBuilder clientToTaskServerConfBuilder = tang.newConfigurationBuilder();
    clientToTaskServerConfBuilder.bindImplementation(ClientToTaskMessage.class, DefaultClientToTaskMessageImpl.class);
    clientToTaskServerConfBuilder.bindConstructor(SpecificResponder.class, ClientToTaskSpecificResponderWrapper.class);
    clientToTaskServerConfBuilder.bindConstructor(Server.class, AvroRPCNettyServerWrapper.class);
    clientToTaskServerConfBuilder.bindNamedParameter(RPCServerPort.class, String.valueOf(clientToTaskPortNum));

    Injector injector = tang.newInjector(clientToTaskServerConfBuilder.build());
    injector.bindVolatileInstance(QueryManager.class, queryManager);
    this.clientToTaskServer = injector.getInstance(Server.class);
    this.queryManager = injector.getInstance(QueryManager.class);
  }

  @Override
  public byte[] call(final byte[] bytes) throws Exception {
    LOG.log(Level.INFO, "MistTask is started");
    countDownLatch.await();
    masterToTaskServer.close();
    clientToTaskServer.close();
    queryManager.close();
    return new byte[0];
  }

  /**
   * A handler for closing the task.
   */
  public final class TaskCloseHandler implements EventHandler<CloseEvent> {
    @Override
    public void onNext(final CloseEvent closeEvent) {
      LOG.log(Level.INFO, "Closing Task");
      countDownLatch.countDown();
    }
  }
}
