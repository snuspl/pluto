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
package edu.snu.mist.core.task;

import edu.snu.mist.common.sources.DataGenerator;
import edu.snu.mist.common.sources.EventGenerator;

/**
 * This class represents the implementation of Source interface.
 * @param <T> the type of input data
 */
final class PhysicalSourceImpl<T> implements PhysicalSource<T> {

  /**
   * Source id.
   */
  private final String sourceId;

  /**
   * Data generator that generates data.
   */
  private final DataGenerator<T> dataGenerator;

  /**
   * Event generator that generates watermark.
   */
  private final EventGenerator<T> eventGenerator;

  public PhysicalSourceImpl(final String sourceId,
                            final DataGenerator<T> dataGenerator, final EventGenerator<T> eventGenerator) {
    this.sourceId = sourceId;
    this.dataGenerator = dataGenerator;
    this.eventGenerator = eventGenerator;
  }

  @Override
  public void start() {
    if (dataGenerator != null && eventGenerator != null) {
      dataGenerator.setEventGenerator(eventGenerator);
      eventGenerator.start();
      dataGenerator.start();
    } else {
      throw new RuntimeException("DataGenerator and EventGenerator should be set in " +
          PhysicalSourceImpl.class.getName());
    }
  }

  @Override
  public void close() throws Exception {
    dataGenerator.close();
    eventGenerator.close();
  }


  @Override
  public String getId() {
    return sourceId;
  }

  @Override
  public DataGenerator<T> getDataGenerator() {
    return dataGenerator;
  }

  @Override
  public EventGenerator<T> getEventGenerator() {
    return eventGenerator;
  }

  @Override
  public Type getType() {
    return Type.SOURCE;
  }
}
