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
package edu.snu.mist.core.driver;

import edu.snu.mist.formats.avro.IPAddress;
import edu.snu.mist.formats.avro.QueryInfo;
import org.apache.avro.AvroRemoteException;

import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A default task selector which returns a list of task ip addresses for client queries.
 * This simply returns the list of tasks without load information about tasks.
 */
final class RandomTaskSelectorImpl implements TaskSelector {

  private List<IPAddress> taskIPAddressList;

  @Inject
  private RandomTaskSelectorImpl() {
    this.taskIPAddressList = new CopyOnWriteArrayList<>();
  }

  @Override
  public void registerRunningTask(final String taskAddress) {

    final String[] splitAddress = taskAddress.split(":");
    final IPAddress ipAddress = new IPAddress();
    ipAddress.setHostAddress(splitAddress[0]);
    ipAddress.setPort(Integer.valueOf(splitAddress[1]));
    taskIPAddressList.add(ipAddress);
  }

  @Override
  public void unregisterTask(final String taskAddress) {

  }

  /**
   * Returns the list of ip addresses of the MistTasks.
   * This method is called by avro RPC when client calls .getTasks(msg);
   * Current implementation simply returns the list of tasks.
   * @param message a message containing query information from clients
   * @return a list of ip addresses of MistTasks
   * @throws AvroRemoteException
   */
  @Override
  public IPAddress getTask(final QueryInfo message) throws AvroRemoteException {
    final int randomIndex = (int)(Math.random() * taskIPAddressList.size());
    return taskIPAddressList.get(randomIndex);
  }
}
