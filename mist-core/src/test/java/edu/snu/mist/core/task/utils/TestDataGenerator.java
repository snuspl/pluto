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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.core.sources.DataGenerator;
import edu.snu.mist.core.sources.EventGenerator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A DataGenerator implementation for testing.
 * It emits a list of strings.
 */
public final class TestDataGenerator<String> implements DataGenerator<String> {

  private final AtomicBoolean closed;
  private final AtomicBoolean started;
  private final ExecutorService executorService;
  private final long sleepTime;
  private EventGenerator eventGenerator;
  private final Iterator<String> inputs;

  /**
   * Generates input data from List and count down the number of input data.
   */
  public TestDataGenerator(final List<String> inputs) {
    this.executorService = Executors.newSingleThreadExecutor();
    this.closed = new AtomicBoolean(false);
    this.started = new AtomicBoolean(false);
    this.sleepTime = 1000L;
    this.inputs = inputs.iterator();
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      executorService.submit(() -> {
        while (!closed.get()) {
          try {
            // fetch an input
            final String input = nextInput();
            if (eventGenerator == null) {
              throw new RuntimeException("EventGenerator should be set in " +
                  TestDataGenerator.class.getName());
            }
            if (input == null) {
              Thread.sleep(sleepTime);
            } else {
              eventGenerator.emitData(input);
            }
          } catch (final IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          } catch (final InterruptedException e) {
            e.printStackTrace();
          }
        }
      });
    }
  }

  public String nextInput() throws IOException {
    if (inputs.hasNext()) {
      final String input = inputs.next();
      return input;
    } else {
      return null;
    }
  }

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      executorService.shutdown();
    }
  }

  @Override
  public void setEventGenerator(final EventGenerator eventGenerator) {
    this.eventGenerator = eventGenerator;
  }
}