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
package edu.snu.mist.core.task.threadpool;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public final class LockConcurrentLinkedQueue<E> implements BlockingQueue<E> {

  private final ReentrantLock lock = new ReentrantLock();
  private final Condition notEmpty = lock.newCondition();

  private final Queue<E> queue;

  public LockConcurrentLinkedQueue() {
    this.queue = new ConcurrentLinkedQueue<>();
  }

  @Override
  public boolean add(final E e) {

    final boolean b = queue.add(e);

    try {
      lock.lock();
      notEmpty.signal();
    } finally {
      lock.unlock();
    }

    return b;
  }

  @Override
  public boolean offer(final E e) {
    return queue.offer(e);
  }

  @Override
  public E remove() {
    return queue.remove();
  }

  @Override
  public void put(final E e) throws InterruptedException {
    throw new RuntimeException("Not supported)");
  }

  @Override
  public boolean offer(final E e, final long timeout, final TimeUnit unit) throws InterruptedException {
    return queue.offer(e);
  }

  @Override
  public E take() throws InterruptedException {
    while (true) {
      if (Thread.currentThread().isInterrupted()) {
        return null;
      }

      E value = queue.poll();
      if (value != null) {
        return value;
      }

      try {
        lock.lock();
        notEmpty.await();
      } finally {
        lock.unlock();
      }
    }
  }

  @Override
  public E poll(final long timeout, final TimeUnit unit) throws InterruptedException {
    throw new RuntimeException("Not supported)");
  }

  @Override
  public int remainingCapacity() {
    throw new RuntimeException("Not supported)");
  }

  @Override
  public boolean remove(final Object o) {
    return queue.remove(o);
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    return queue.containsAll(c);
  }

  @Override
  public boolean addAll(final Collection<? extends E> c) {
    return queue.addAll(c);
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    return queue.removeAll(c);
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    return retainAll(c);
  }

  @Override
  public void clear() {
    queue.clear();
  }

  @Override
  public int size() {
    return queue.size();
  }

  @Override
  public boolean isEmpty() {
    return queue.isEmpty();
  }

  @Override
  public boolean contains(final Object o) {
    return queue.contains(o);
  }

  @NotNull
  @Override
  public Iterator<E> iterator() {
    return queue.iterator();
  }

  @NotNull
  @Override
  public Object[] toArray() {
    return queue.toArray();
  }

  @NotNull
  @Override
  public <T> T[] toArray(final T[] a) {
    return queue.toArray(a);
  }

  @Override
  public int drainTo(final Collection<? super E> c) {
    throw new RuntimeException("Not supported)");
  }

  @Override
  public int drainTo(final Collection<? super E> c, final int maxElements) {
    throw new RuntimeException("Not supported)");
  }

  @Override
  public E poll() {
    return queue.poll();
  }

  @Override
  public E element() {
    return queue.element();
  }

  @Override
  public E peek() {
    return queue.peek();
  }

}