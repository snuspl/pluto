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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.MasterHostname;
import edu.snu.mist.core.parameters.TaskId;
import edu.snu.mist.core.parameters.TaskToMasterPort;
import edu.snu.mist.core.rpc.AvroUtils;
import edu.snu.mist.core.task.groupaware.GroupIdRequestor;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * The default group id requestor implementation class.
 */
public final class DefaultGroupIdRequestorImpl implements GroupIdRequestor {

  /**
   * The task id of the current task.
   */
  private String taskId;

  /**
   * The task-to-master avro proxy.
   */
  private TaskToMasterMessage proxyToMaster;

  @Inject
  private DefaultGroupIdRequestorImpl(
      @Parameter(MasterHostname.class) final String masterHostname,
      @Parameter(TaskId.class) final String taskId,
      @Parameter(TaskToMasterPort.class) final int taskToMasterPort) throws IOException {
    this.taskId = taskId;
    this.proxyToMaster = AvroUtils.createAvroProxy(TaskToMasterMessage.class,
        new InetSocketAddress(masterHostname, taskToMasterPort));
  }

  @Override
  public String requestGroupId(final String appId) {
    try {
      return proxyToMaster.createGroup(taskId, appId);
    } catch (final AvroRemoteException e) {
      e.printStackTrace();
      return null;
    }
  }
}
