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

package edu.snu.mist.launcher;

import org.apache.reef.client.LauncherStatus;
import org.junit.Assert;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

/**
 * The test class for MistLauncher.
 */
public class MistLauncherTest {
  /**
   * Test for MistLauncher.
   * @throws InjectionException
   */
  @Test
  public void testMistLauncher() throws InjectionException {
    final LauncherStatus status = MistLauncher
        .getLauncher(MistLauncher.RuntimeType.LOCAL)
        .setTimeout(1000)
        .run(1, 1, 1, 7777, 2048);
    Assert.assertEquals(LauncherStatus.FORCE_CLOSED, status);
  }
}
