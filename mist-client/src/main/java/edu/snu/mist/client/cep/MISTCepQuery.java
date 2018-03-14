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
package edu.snu.mist.client.cep;

import edu.snu.mist.common.operators.CepEventPattern;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Default Implementation for MISTCepQuery.
 */
public final class MISTCepQuery<T> {
  private final String superGroupId;
  private final String subGroupId;
  private final CepInput<T> cepInput;
  private final List<CepEventPattern<T>> cepEventPatternSequence;
  private final CepQualifier<T> cepQualifier;
  private final long windowTime;
  private final CepAction cepAction;

  /**
   * Creates an immutable MISTCepQuery using given parameters.
   * @param superGroupId            super group id
   * @param subGroupId              sub group id
   * @param cepInput                cep input
   * @param cepEventPatternSequence sequence of event
   * @param cepQualifier            pattern qualification
   * @param cepAction               cep action
   */
  private MISTCepQuery(
      final String superGroupId,
      final String subGroupId,
      final CepInput<T> cepInput,
      final List<CepEventPattern<T>> cepEventPatternSequence,
      final CepQualifier<T> cepQualifier,
      final long windowTime,
      final CepAction cepAction) {
    this.superGroupId = superGroupId;
    this.subGroupId = subGroupId;
    this.cepInput = cepInput;
    this.cepEventPatternSequence = cepEventPatternSequence;
    this.cepQualifier = cepQualifier;
    this.windowTime = windowTime;
    this.cepAction = cepAction;
  }

  public String getSuperGroupId() {
    return superGroupId;
  }

  public String getSubGroupId() {
    return subGroupId;
  }

  public CepInput<T> getCepInput() {
    return cepInput;
  }

  public List<CepEventPattern<T>> getCepEventPatternSequence() {
    return cepEventPatternSequence;
  }

  public CepQualifier<T> getCepQualifier() {
    return cepQualifier;
  }

  public long getWindowTime() {
    return windowTime;
  }

  public CepAction getCepAction() {
    return cepAction;
  }

  /**
   * A builder class for MISTCepQuery.
   */
  public static class Builder<T> {
    private final String superGroupId;
    private final String subGroupId;
    private CepInput<T> cepInput;
    private List<CepEventPattern<T>> cepEventPatternSequence;
    private CepQualifier<T> cepQualifier;
    private long windowTime;
    private CepAction cepAction;

    public Builder(final String superGroupId, final String subGroupId) {
      this.superGroupId = superGroupId;
      this.subGroupId = subGroupId;
      this.cepInput = null;
      this.cepEventPatternSequence = null;
      this.cepQualifier = null;
      this.windowTime = -1L;
      this.cepAction = null;
    }

    /**
     * Define an input of the CEP query.
     * @param input input for this query
     * @return builder
     */
    public Builder input(final CepInput<T> input) {
      if (this.cepInput != null) {
        throw new IllegalStateException("Input couldn't be declared twice!");
      }
      this.cepInput = input;
      return this;
    }

    /**
     * Define an event sequence of the CEP query.
     * @param events input cep events
     * @return builder
     */
    public Builder setEventPatternSequence(final CepEventPattern<T>... events) {
      cepEventPatternSequence = Arrays.asList(events);
      return this;
    }

    /**
     * Define an qualification of the candidate sequences.
     * @param cepQualifierParam input qualification
     * @return builder
     */
    public Builder setQualifier(final CepQualifier cepQualifierParam) {
      cepQualifier = cepQualifierParam;
      return this;
    }

    /**
     * Define a window time of sequence.
     * @param time window time(millisecond)
     * @return builder
     */
    public Builder within(final long time) {
      windowTime = time;
      return this;
    }

    /**
     * Define an Action of CEP query.
     * @param action input cep query
     * @return builder
     */
    public Builder setAction(final CepAction action) {
      cepAction = action;
      return this;
    }

    /**
     * Check the differences of events' name.
     * @return boolean
     */
    private boolean checkSequence() {
      // name check
      final Set<String> eventNameSet = new HashSet<>();
      cepEventPatternSequence.stream()
          .forEach(e -> eventNameSet.add(e.getEventPatternName()));
      return cepEventPatternSequence.size() == eventNameSet.size();
    }

    public MISTCepQuery<T> build() {
      if (superGroupId == null
          || subGroupId == null
          || cepInput == null
          || cepEventPatternSequence == null
          || cepQualifier == null
          || windowTime < 0
          || cepAction == null) {
        throw new IllegalStateException(
            "One of group id, input, event pattern sequence, qualifier, window, or action is not set!");
      }
      if (!checkSequence()) {
        throw new IllegalStateException("Each event should have a different event name!");
      }
      return new MISTCepQuery(superGroupId, subGroupId, cepInput,
          cepEventPatternSequence, cepQualifier, windowTime, cepAction);
    }
  }
}
