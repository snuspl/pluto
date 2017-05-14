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

import edu.snu.mist.core.parameters.OperatorIdPrefix;
import edu.snu.mist.core.parameters.OperatorChainIdPrefix;
import edu.snu.mist.core.parameters.SinkIdPrefix;
import edu.snu.mist.core.parameters.SourceIdPrefix;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Default implementation class for IdGenerator.
 */
final class DefaultIdGeneratorImpl implements IdGenerator {

  /**
   * Prefix used for generating operator Ids.
   */
  private final String operatorPrefix;

  /**
   * Prefix used for generating operatorChain Ids.
   */
  private final String operatorChainPrefix;

  /**
   * Prefix used for generating source Ids.
   */
  private final String sourcePrefix;

  /**
   * Prefix used for generating sink Ids.
   */
  private final String sinkPrefix;


  /**
   * Atomic ID Sequence number used for generating operator Ids.
   */
  private final AtomicLong operatorIdSeqNum;

  /**
   * Atomic ID Sequence number used for generating operatorChain Ids.
   */
  private final AtomicLong operatorChainIdSeqNum;

  /**
   * Atomic ID Sequence number used for generating source Ids.
   */
  private final AtomicLong sourceIdSeqNum;

  /**
   * Atomic ID Sequence number used for generating sink Ids.
   */
  private final AtomicLong sinkIdSeqNum;

  @Inject
  private DefaultIdGeneratorImpl(@Parameter(OperatorIdPrefix.class) final String operatorPrefix,
                                 @Parameter(OperatorChainIdPrefix.class) final String operatorChainPrefix,
                                 @Parameter(SourceIdPrefix.class) final String sourcePrefix,
                                 @Parameter(SinkIdPrefix.class) final String sinkPrefix) {
    this.operatorPrefix = operatorPrefix;
    this.operatorChainPrefix = operatorChainPrefix;
    this.sourcePrefix = sourcePrefix;
    this.sinkPrefix = sinkPrefix;
    this.operatorIdSeqNum = new AtomicLong();
    this.operatorChainIdSeqNum = new AtomicLong();
    this.sourceIdSeqNum = new AtomicLong();
    this.sinkIdSeqNum = new AtomicLong();
  }

  @Override
  public String generateOperatorId() {
    final StringBuilder sb = new StringBuilder();
    sb.append(operatorPrefix);
    sb.append(operatorIdSeqNum.getAndIncrement());
    return sb.toString();
  }

  @Override
  public String generateOperatorChainId() {
    final StringBuilder sb = new StringBuilder();
    sb.append(operatorChainPrefix);
    sb.append(operatorChainIdSeqNum.getAndIncrement());
    return sb.toString();
  }


  @Override
  public String generateSourceId() {
    final StringBuilder sb = new StringBuilder();
    sb.append(sourcePrefix);
    sb.append(sourceIdSeqNum.getAndIncrement());
    return sb.toString();
  }

  @Override
  public String generateSinkId() {
    final StringBuilder sb = new StringBuilder();
    sb.append(sinkPrefix);
    sb.append(sinkIdSeqNum.getAndIncrement());
    return sb.toString();
  }
}