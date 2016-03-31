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

import javax.inject.Inject;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Multiple queue for our new model.
 */
public final class MultiQueue implements SchedulingQueue {


  @Inject
  private MultiQueue() {
  }


  @Override
  public boolean add(final Runnable runnable) {
    return false;
  }

  @Override
  public boolean offer(final Runnable r) {
    return false;
  }

  @Override
  public Runnable remove() {
    return null;
  }

  @Override
  public Runnable poll() {
    return null;
  }

  @Override
  public Runnable element() {
    return null;
  }

  @Override
  public Runnable peek() {
    return null;
  }

  @Override
  public void put(final Runnable runnable) throws InterruptedException {

  }

  @Override
  public boolean offer(final Runnable runnable, final long timeout, final TimeUnit unit) throws InterruptedException {
    return false;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  @Override
  public int drainTo(final Collection<? super Runnable> c) {
    return 0;
  }

  @Override
  public int drainTo(final Collection<? super Runnable> c, final int maxElements) {
    return 0;
  }

  @Override
  public boolean remove(final Object o) {
    return false;
  }

  @Override
  public boolean containsAll(final Collection<?> c) {
    return false;
  }

  @Override
  public boolean addAll(final Collection<? extends Runnable> c) {
    return false;
  }

  @Override
  public boolean removeAll(final Collection<?> c) {
    return false;
  }

  @Override
  public boolean retainAll(final Collection<?> c) {
    return false;
  }

  @Override
  public void clear() {

  }

  @Override
  public boolean contains(final Object o) {
    return false;
  }

  @Override
  public Iterator<Runnable> iterator() {
    return null;
  }

  @Override
  public Object[] toArray() {
    return new Object[0];
  }

  @Override
  public <T> T[] toArray(final T[] a) {
    return null;
  }

  @Override
  public Runnable poll(final long timeout, final TimeUnit unit) throws InterruptedException {
    return null;
  }

  @Override
  public int remainingCapacity() {
    return 0;
  }

  @Override
  public Runnable take() throws InterruptedException {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }
}
