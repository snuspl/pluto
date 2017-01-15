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

import java.util.Arrays;
import java.util.List;

/**
 * An immutable action which does something (or nothing) to its sink.
 */
public final class CepAction {

  private CepActionType actionType;
  private final CepSink cepSink;
  private final List<Object> params;

  // Should not be public!
  private CepAction(final CepActionType actionType, final CepSink cepSink, final Object... params) {
    this.actionType = actionType;
    this.cepSink = cepSink;
    this.params = Arrays.asList(params);
  }

  public static CepAction doNothingAction() {
    return new CepAction(CepActionType.DO_NOTHING, null, "dummy");
  }

  /**
   * Creates a new immutable action based on given parameters.
   * @param actionType
   * @param cepSink
   * @param params
   * @return
   */
  public static CepAction newAction(final CepActionType actionType, final CepSink cepSink,  final Object... params) {
    if (cepSink == null) {
      throw new IllegalStateException("CepSink cannot be null!");
    }
    return new CepAction(actionType, cepSink, params);
  }

  /**
   * @return the type of this action
   */
  public CepActionType getCepActionType() {
    return actionType;
  }

  /**
   * @return the target sink of this action
   */
  public CepSink getCepSink() {
    return cepSink;
  }

  /**
   * @return parameters necessary for this action
   */
  public List<Object> getParams() {
    return params;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof CepAction)) {
      return false;
    }
    final CepAction action = (CepAction) o;
    if (this.cepSink == null || action.cepSink == null) {
      if (this.cepSink != null || action.cepSink != null) {
        return false;
      } else {
        return this.actionType.equals(action.actionType) &&
            this.params.equals(action.params);
      }
    } else {
      return this.actionType.equals(action.actionType)
          && this.cepSink.equals(action.cepSink)
          && this.params.equals(action.params);
    }
  }

  @Override
  public int hashCode() {
    if (cepSink != null) {
      return actionType.hashCode() * 100 + cepSink.hashCode() * 10 + params.hashCode();
    } else {
      return actionType.hashCode() * 100 + params.hashCode();
    }
  }
}