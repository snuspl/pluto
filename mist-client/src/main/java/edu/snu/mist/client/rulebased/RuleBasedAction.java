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
package edu.snu.mist.client.rulebased;

import java.util.ArrayList;
import java.util.List;

/**
 * An immutable action which does something (or nothing) to its sink.
 */
public final class RuleBasedAction {

  private final RuleBasedActionType actionType;
  private final RuleBasedSink sink;
  private final List<Object> params;

  // Should not be public!
  private RuleBasedAction(final RuleBasedActionType actionType,
                          final RuleBasedSink sink,
                          final List<Object> params) {
    this.actionType = actionType;
    this.sink = sink;
    this.params = params;
  }

  /**
   * @return A pre-define action which does nothing!
   */
  public static RuleBasedAction doNothingAction() {
      final List<Object> list = new ArrayList<>();
      list.add("dummy");
      return new RuleBasedAction(RuleBasedActionType.DO_NOTHING, null, list);
  }

  /**
   * Creates a new immutable action based on given parameters.
   * @param actionTypeArg the action type
   * @param sinkArg the sink to commit action
   * @param paramsArg parameters necessary for actions
   * @return new action
   */
  private RuleBasedAction newAction(final RuleBasedActionType actionTypeArg,
                              final RuleBasedSink sinkArg,
                              final List<Object> paramsArg) {
    if (sinkArg == null) {
      throw new IllegalStateException("RuleBasedSink cannot be null!");
    }
    return new RuleBasedAction(actionTypeArg, sinkArg, paramsArg);
  }

  /**
   * @return the type of this action
   */
  public RuleBasedActionType getActionType() {
    return actionType;
  }

  /**
   * @return the target sink of this action
   */
  public RuleBasedSink getSink() {
    return sink;
  }

  /**
   * @return parameters necessary for this action
   */
  public List<Object> getParams() {
    return params;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof RuleBasedAction)) {
      return false;
    }
    final RuleBasedAction action = (RuleBasedAction) o;
    if (this.sink == null || action.sink == null) {
      if (this.sink != null || action.sink != null) {
        return false;
      } else {
        return this.actionType.equals(action.actionType) &&
            this.params.equals(action.params);
      }
    } else {
      return this.actionType.equals(action.actionType)
          && this.sink.equals(action.sink)
          && this.params.equals(action.params);
    }
  }

  @Override
  public int hashCode() {
    if (sink != null) {
      return actionType.hashCode() * 100 + sink.hashCode() * 10 + params.hashCode();
    } else {
      return actionType.hashCode() * 100 + params.hashCode();
    }
  }

  /**
   * A builder class for RuleBasedAction.
   */
  public static class Builder {
    private RuleBasedActionType actionType;
    private RuleBasedSink sink;
    private final List<Object> params;

    public Builder() {
      actionType = null;
      sink = null;
      params = new ArrayList<>();
    }

    /**
     * @param actionTypeParam an action type
     * @return builder
     */
    public Builder setActionType(final RuleBasedActionType actionTypeParam) {
      if (this.actionType != null) {
        throw new IllegalStateException("actionTypeParam cannot be defined twice!");
      }
      this.actionType = actionTypeParam;
      return this;
    }

    /**
     * @param sinkParam a rule-based sink
     * @return builder
     */
    public Builder setSink(final RuleBasedSink sinkParam) {
      if (this.sink != null) {
        throw new IllegalStateException("SinkParam cannot be defined twice");
      }
      this.sink = sinkParam;
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
     * @return a rule-based action built via this builder.
     */
    public RuleBasedAction build() {
      return new RuleBasedAction(actionType, sink, params);
    }
  }
}