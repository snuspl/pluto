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
package edu.snu.mist.api.cep;

import java.util.List;

/**
 * Default Implementation for MISTCepStatefulQuery.
 */
public class MISTCepStatefulQueryImpl implements MISTCepStatefulQuery {

  private final CepInput cepInput;
  private final String initialState;
  private final List<CepStatefulRule> cepStatefulRules;

  /**
   * Creates an immutable MISTCepStatefulQuery using given parameters.
   * @param cepInput
   * @param initialState
   * @param cepStatefulRules
   */
  public MISTCepStatefulQueryImpl(
      final CepInput cepInput,
      final String initialState,
      final List<CepStatefulRule> cepStatefulRules) {
    this.cepInput = cepInput;
    this.initialState = initialState;
    this.cepStatefulRules = cepStatefulRules;
  }

  @Override
  public CepInput getCepInput() {
    return cepInput;
  }

  @Override
  public List<CepStatefulRule> getCepStatefulRules() {
    return cepStatefulRules;
  }

  @Override
  public String getInitialState() {
    return initialState;
  }
}
