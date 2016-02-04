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
package edu.snu.mist.api.window;

/**
 * Window policy interface which decides the size of the window inside.
 */
public interface WindowSizePolicy {
  // TODO[MIST-76]: Support UDF WindowMaintainOption for more flexible window operation design.
  /**
   * @return The type of window size policy type.
   */
  WindowType.SizePolicy getSizePolicyType();
}