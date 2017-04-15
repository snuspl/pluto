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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.core.task.ThreadManager;

import java.util.Set;

/**
 * This is the thread manager for test.
 */
public final class TestThreadManager implements ThreadManager {

  public TestThreadManager() {
    // do nothing
  }

  @Override
  public Set<Thread> getThreads() {
    return null;
  }

  @Override
  public void setThreadNum(final int threadNum) {
    // do nothing
  }

  @Override
  public boolean reapCheck() {
    return false;
  }

  @Override
  public void close() throws Exception {
    // do nothing
  }
}