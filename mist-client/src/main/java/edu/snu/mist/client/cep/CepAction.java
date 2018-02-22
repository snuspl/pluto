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

import java.util.ArrayList;
import java.util.List;

/**
 * An immutable action which does something (or nothing) to its sink.
 */
public final class CepAction {

  private final CepActionType actionType;
  private final CepSink cepSink;
  private final List<Object> params;

  // Should not be public!
  private CepAction(final CepActionType actionType, final CepSink cepSink, final List<Object> params) {
    this.actionType = actionType;
    this.cepSink = cepSink;
    this.params = params;
  }

  /**
   * @return A pre-define action which does nothing!
   */
  public static CepAction doNothingAction() {
      final List<Object> list = new ArrayList<>();
      list.add("dummy");
      return new CepAction(CepActionType.DO_NOTHING, null, list);
  }

  /**
   * Creates a new immutable action based on given parameters.
   * @param actionTypeArg the action type
   * @param cepSinkArg the sink to commit action
   * @param paramsArg parameters necessary for actions
   * @return new action
   */
  private CepAction newAction(final CepActionType actionTypeArg,
                              final CepSink cepSinkArg,
                              final List<Object> paramsArg) {
    if (cepSinkArg == null) {
      throw new IllegalStateException("CepSink cannot be null!");
    }
    return new CepAction(actionTypeArg, cepSinkArg, paramsArg);
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

  /**
   * A builder class for CepAction.
   */
  public static class Builder {
    private CepActionType actionType;
    private CepSink cepSink;
    private final List<Object> params;

    public Builder() {
      actionType = null;
      cepSink = null;
      params = new ArrayList<>();
    }

    /**
     * @param actionTypeParam an action type
     * @return builder
     */
    public Builder setActionType(final CepActionType actionTypeParam) {
      if (this.actionType != null) {
        throw new IllegalStateException("actionTypeParam cannot be defined twice!");
      }
      this.actionType = actionTypeParam;
      return this;
    }

    /**
     * @param cepSinkParam a cep sink
     * @return builder
     */
    public Builder setCepSink(final CepSink cepSinkParam) {
      if (this.cepSink != null) {
        throw new IllegalStateException("cepSinkParam cannot be defined twice");
      }
      this.cepSink = cepSinkParam;
      return this;
    }

    /**
     * @param object a parameter
     * @return builder
     */
    public Builder setParams(final Object object) {
      this.params.add(object);
      return this;
    }

    /**
     * @return a cep action built via this builder.
     */
    public CepAction build() {
      return new CepAction(actionType, cepSink, params);
    }
  }
}