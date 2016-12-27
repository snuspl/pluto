/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.common.windows;

import edu.snu.mist.formats.avro.WindowOperatorInfo;

/**
 * This interface represents an information container that has some information used during windowing operation.
 * During windowed stream creation, this information class will be passed as a parameter.
 */
public interface WindowInformation {

  /**
   * @return the serialized window operator information.
   */
  WindowOperatorInfo getSerializedWindowOpInfo();
}
