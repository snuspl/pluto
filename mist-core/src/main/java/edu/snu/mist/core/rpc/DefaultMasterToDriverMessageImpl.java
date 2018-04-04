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
package edu.snu.mist.core.rpc;

import edu.snu.mist.core.driver.MistTaskSubmitInfoStore;
import edu.snu.mist.formats.avro.MasterToDriverMessage;
import edu.snu.mist.formats.avro.TaskRequest;
import org.apache.avro.AvroRemoteException;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;
import org.apache.reef.tang.formats.ConfigurationSerializer;

import javax.inject.Inject;
import java.io.IOException;

/**
 * The default master to driver message server implementation.
 */
public final class DefaultMasterToDriverMessageImpl implements MasterToDriverMessage {

  /**
   * The MistDriver's evaluator requestor.
   */
  private final EvaluatorRequestor requestor;

  /**
   * The shared store for mist task configuration.
   */
  private final MistTaskSubmitInfoStore taskSubmitInfoStore;

  /**
   * The serializer for Tang configuration.
   */
  private final ConfigurationSerializer configurationSerializer;

  @Inject
  private DefaultMasterToDriverMessageImpl(
      final MistTaskSubmitInfoStore taskSubmitInfoStore,
      final EvaluatorRequestor requestor) {
    this.taskSubmitInfoStore = taskSubmitInfoStore;
    this.requestor = requestor;
    this.configurationSerializer = new AvroConfigurationSerializer();
  }

  @Override
  public synchronized Void requestNewTask(final TaskRequest taskRequest) throws AvroRemoteException {
    try {
      final Configuration conf =
          configurationSerializer.fromString(taskRequest.getSerializedTaskConfiguration());
      // Store task submit info.
      taskSubmitInfoStore.setTaskConfiguration(conf);
      taskSubmitInfoStore.setNewRatio(taskRequest.getNewRatio());
      taskSubmitInfoStore.setReservedCodeCacheSize(taskRequest.getReservedCodeCacheSize());
      // Submit an evaluator requst for tasks.
      requestor.newRequest()
          .setNumber(taskRequest.getTaskNum())
          .setNumberOfCores(taskRequest.getTaskCpuNum())
          .setMemory(taskRequest.getTaskMemSize())
          .submit();
    } catch (final IOException e) {
      e.printStackTrace();
      throw new AvroRemoteException("Cannot deserialize the task configuration!");
    }
    return null;
  }
}
