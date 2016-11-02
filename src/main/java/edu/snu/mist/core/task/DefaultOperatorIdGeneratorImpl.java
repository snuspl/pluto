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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.OperatorIdPrefix;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation class for OperatorIdGenerator.
 */
final class DefaultOperatorIdGeneratorImpl implements OperatorIdGenerator {

  /**
   * Prefix used for generating operator Ids.
   */
  private final String prefix;

  /**
   * Atomic ID Sequence number used for generating operator Ids.
   */
  private final AtomicLong operatorIdSeqNum;

  @Inject
  private DefaultOperatorIdGeneratorImpl(@Parameter(OperatorIdPrefix.class) final String prefix) {
    this.prefix = prefix;
    this.operatorIdSeqNum = new AtomicLong();
  }

  @Override
  public String generate() {
    final StringBuilder sb = new StringBuilder();
    sb.append(prefix);
    sb.append(operatorIdSeqNum.getAndIncrement());
    return sb.toString();
  }
}