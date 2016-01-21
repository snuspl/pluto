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
package edu.snu.mist.task.executor.queues;

import org.apache.reef.tang.annotations.DefaultImplementation;

import java.util.Collection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * This is an interface of a queue of MistExecutor.
 * The methods actually used in ThreadPoolExecutor are
 * 'offer', 'isEmpty', 'drainTo', 'remove', 'poll', 'take' and 'size'.
 * By implementing these methods, this queue can schedule the submitted jobs.
 */
@DefaultImplementation(FIFOQueue.class)
public interface SchedulingQueue extends BlockingQueue<Runnable> {

  @Override
  boolean offer(Runnable r);

  @Override
  boolean isEmpty();

  @Override
  int drainTo(Collection<? super Runnable> c);

  @Override
  boolean remove(Object o);

  @Override
  Runnable poll(long timeout, TimeUnit unit) throws InterruptedException;

  @Override
  Runnable take() throws InterruptedException;

  @Override
  int size();
}