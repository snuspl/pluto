/*
 * Copyright (C) 2017 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.core.task;

import edu.snu.mist.core.master.parameters.TaskToMasterServerPortNum;
import edu.snu.mist.core.parameters.MasterHostAddress;
import edu.snu.mist.core.parameters.MasterToTaskServerPortNum;
import edu.snu.mist.core.task.eventProcessors.EventProcessor;
import edu.snu.mist.core.task.eventProcessors.GroupAllocationTable;
import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.TaskLoadInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public final class DefaultTaskLoadManager implements TaskLoadManager {

  private final GroupAllocationTable groupAllocationTable;
  private final TaskToMasterMessage taskToMasterMessage;
  private final int masterToTaskServerPortNum;

  @Inject
  private DefaultTaskLoadManager(final GroupAllocationTable groupAllocationTable,
                                 @Parameter(MasterHostAddress.class) final String masterHostAddress,
                                 @Parameter(TaskToMasterServerPortNum.class) final int taskToMasterPortNum,
                                 @Parameter(MasterToTaskServerPortNum.class) final int masterToTaskPortNum)
      throws IOException {
    this.groupAllocationTable = groupAllocationTable;
    final NettyTransceiver taskToMaster =
        new NettyTransceiver(new InetSocketAddress(masterHostAddress, taskToMasterPortNum));
    this.taskToMasterMessage = SpecificRequestor.getClient(TaskToMasterMessage.class, taskToMaster);
    this.masterToTaskServerPortNum = masterToTaskPortNum;
  }

  @Override
  public void sendLoadToMaster() throws AvroRemoteException, UnknownHostException {
    final TaskLoadInfo taskLoadInfo = new TaskLoadInfo();
    final IPAddress ipAddress = new IPAddress();

    ipAddress.setHostAddress(InetAddress.getLocalHost().getHostName());
    ipAddress.setPort(masterToTaskServerPortNum);

    taskLoadInfo.setGroupLoadMap(null);
    taskLoadInfo.setTaskIPAddress(ipAddress);
    taskLoadInfo.setTotalLoad(getTaskLoad());
    taskLoadInfo.setMeasurementTime(System.currentTimeMillis());
    taskToMasterMessage.sendTaskLoadInfo(taskLoadInfo);
  }

  // TODO: Get group load
  private double getTaskLoad() {
    double totalLoad = 0;
    for (final EventProcessor eventProcessor : groupAllocationTable.getKeys()) {
      totalLoad += eventProcessor.getLoad();
    }
    return totalLoad / groupAllocationTable.size();
  }
}
