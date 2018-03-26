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

import edu.snu.mist.formats.avro.GroupStats;
import edu.snu.mist.formats.avro.RecoveryInfo;
import edu.snu.mist.formats.avro.TaskToMasterMessage;
import org.apache.avro.AvroRemoteException;

/**
 * The mock master server used in groupaware test package.
 */
public final class MockTaskToMasterMessage implements TaskToMasterMessage {

  private static final String GROUP_NAME = "group";

  public MockTaskToMasterMessage() {
    // Do nothing here.
  }

  @Override
  public String createGroup(final String taskHostname, final GroupStats groupStats) throws AvroRemoteException {
    return GROUP_NAME;
  }

  @Override
  public RecoveryInfo getRecoveringGroups(final String taskHostname) throws AvroRemoteException {
    return null;
  }
}
