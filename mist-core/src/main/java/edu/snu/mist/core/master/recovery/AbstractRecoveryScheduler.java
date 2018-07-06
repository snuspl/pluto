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
package edu.snu.mist.core.master.recovery;

import java.util.concurrent.locks.ReentrantLock;

/**
 * The abstract implementation of RecoveryScheduler with lcoks.
 */
public abstract class AbstractRecoveryScheduler implements RecoveryScheduler {

  protected ReentrantLock recoveryLock;

  protected AbstractRecoveryScheduler() {
    this.recoveryLock = new ReentrantLock();
  }

  @Override
  public void acquireRecoveryLock() {
    this.recoveryLock.lock();
  }

  @Override
  public boolean tryAcquireRecoveryLock() {
    return this.recoveryLock.tryLock();
  }

  @Override
  public void releaseRecoveryLock() {
    this.recoveryLock.unlock();
  }
}
