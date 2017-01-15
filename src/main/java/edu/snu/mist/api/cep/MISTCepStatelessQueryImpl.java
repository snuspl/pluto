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
 * A default implmenetation class for MISTCepStateless query.
 */
public class MISTCepStatelessQueryImpl implements MISTCepStatelessQuery {

  private final CepInput cepInput;
  private final List<CepStatelessRule> cepStatelessRules;

  /**
   * Creates an immutable stateless query.
   */
  public MISTCepStatelessQueryImpl(final CepInput cepInput,
                                   final List<CepStatelessRule> cepStatelessRules) {
    this.cepInput = cepInput;
    this.cepStatelessRules = cepStatelessRules;
  }

  /**
   * @return input for this query.
   */
  @Override
  public CepInput getCepInput() {
    return cepInput;
  }

  /**
   * @return list of all cep stateless rules.
   */
  @Override
  public List<CepStatelessRule> getCepStatelessRules() {
    return this.cepStatelessRules;
  }
}