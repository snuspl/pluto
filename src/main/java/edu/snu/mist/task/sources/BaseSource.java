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
package edu.snu.mist.task.sources;

import edu.snu.mist.task.common.OutputEmitter;
import org.apache.reef.wake.Identifier;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This is base class for Source.
 * This uses a single dedicated thread to fetch data from the socket server.
 * But, if the number of socket stream generator increases, this thread allocation could be a bottleneck.
 * TODO[MIST-152]: Threads of Source should be managed judiciously
 */
public abstract class BaseSource<I> implements Source<I> {
  /**
   * An output emitter.
   */
  protected OutputEmitter<I> outputEmitter;

  /**
   * A flag for close.
   */
  protected final AtomicBoolean closed;

  /**
   * A flag for start.
   */
  protected final AtomicBoolean started;

  /**
   * An executor service running this source generator.
   * TODO[MIST-152]: Threads of Source should be managed judiciously.
   */
  private final ExecutorService executorService;

  /**
   * Time to sleep when fetched data is null.
   */
  private final long sleepTime;

  /**
   * Identifier of Source.
   */
  protected final Identifier identifier;

  /**
   * Identifier of Query.
   */
  protected final Identifier queryId;

  public BaseSource(final long sleepTime,
                    final Identifier queryId,
                    final Identifier identifier) {
    // TODO[MIST-152]: Threads of Source should be managed judiciously.
    this.executorService = Executors.newSingleThreadExecutor();
    this.closed = new AtomicBoolean(false);
    this.started = new AtomicBoolean(false);
    this.sleepTime = sleepTime;
    this.queryId = queryId;
    this.identifier = identifier;
  }

  @Override
  public void start() {
    if (started.compareAndSet(false, true)) {
      executorService.submit(() -> {
        while (!closed.get()) {
          try {
            // fetch an input
            final I input = nextInput();
            if (outputEmitter == null) {
              throw new RuntimeException("OutputEmitter should be set in " +
                  BaseSource.class.getName());
            }
            if (input == null) {
              Thread.sleep(sleepTime);
            } else {
              outputEmitter.emit(input);
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

  /**
   * Gets next input.
   * @return input
   * @throws IOException
   */
  public abstract I nextInput() throws IOException;


  /**
   * Releases IO resources.
   * This method is called just once.
   * @throws Exception
   */
  public abstract void releaseResources() throws Exception;

  @Override
  public void close() throws Exception {
    if (closed.compareAndSet(false, true)) {
      releaseResources();
      executorService.shutdown();
    }
  }

  @Override
  public Identifier getIdentifier() {
    return identifier;
  }

  @Override
  public Identifier getQueryIdentifier() {
    return queryId;
  }

  @Override
  public void setOutputEmitter(final OutputEmitter<I> emitter) {
    this.outputEmitter = emitter;
  }
}
