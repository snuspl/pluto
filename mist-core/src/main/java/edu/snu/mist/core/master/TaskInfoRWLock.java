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
package edu.snu.mist.core.master;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The shared read/write lock used for synchronizing adding / deleting task info
 * based on ReentrantReadWriteLock class.
 */
public final class TaskInfoRWLock extends ReentrantReadWriteLock {

  private static final Logger LOG = Logger.getLogger(TaskInfoRWLock.class.getName());

  private final LoggableReadLock loggableReadLock;

  private final LoggableWriteLock loggableWriteLock;

  // The injectable constructor.
  @Inject
  private TaskInfoRWLock() {
    super();
    // Do nothing.
    this.loggableReadLock = new LoggableReadLock(this);
    this.loggableWriteLock = new LoggableWriteLock(this);
  }

  /**
   * This method utilizes getReadHoldCount() for determining
   * whether the current thread is holding a read lock or not.
   * @return true / false
   */
  public boolean isReadHoldByCurrentThread() {
    return this.getReadHoldCount() > 0 || this.isWriteLockedByCurrentThread();
  }

  @Override
  public WriteLock writeLock() {
    return loggableWriteLock;
  }

  @Override
  public ReadLock readLock() {
    return loggableReadLock;
  }

  private static class LoggableReadLock extends ReadLock {
    public LoggableReadLock(final ReentrantReadWriteLock lock) {
      super(lock);
    }

    @Override
    public void lock() {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Acquiring Readlock on TaskInfoRWLock: {0}",
            Arrays.toString(Thread.currentThread().getStackTrace()));
      }
      super.lock();
    }

    @Override
    public void unlock() {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Releasing Readlock on TaskInfoRWLock: {0}",
            Arrays.toString(Thread.currentThread().getStackTrace()));
      }
      super.unlock();
    }
  }

  private static class LoggableWriteLock extends WriteLock {
    public LoggableWriteLock(final ReentrantReadWriteLock lock) {
      super(lock);
    }

    @Override
    public void lock() {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Acquiring Writelock on TaskInfoRWLock: {0}",
            Arrays.toString(Thread.currentThread().getStackTrace()));
      }
      super.lock();
    }

    @Override
    public void unlock() {
      if (LOG.isLoggable(Level.FINE)) {
        LOG.log(Level.FINE, "Releasing Readlock on TaskInfoRWLock: {0}",
            Arrays.toString(Thread.currentThread().getStackTrace()));
      }
      super.unlock();
    }
  }
}
