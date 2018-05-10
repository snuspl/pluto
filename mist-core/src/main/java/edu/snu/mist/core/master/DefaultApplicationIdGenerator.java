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

import edu.snu.mist.core.parameters.MasterIndex;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The default application id generator.
 */
public final class DefaultApplicationIdGenerator implements ApplicationIdGenerator {

  /**
   * The atomic integer used for generating a unique app id.
   */
  private final AtomicInteger appIndex;

  /**
   * The index for the mist master.
   */
  private final int masterIndex;

  @Inject
  private DefaultApplicationIdGenerator(@Parameter(MasterIndex.class) final int masterIndex) {
    this.appIndex = new AtomicInteger(0);
    this.masterIndex = masterIndex;
  }

  @Override
  public String generate() {
    return String.format("%d_%d", masterIndex, appIndex.getAndIncrement());
  }
}
