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

import edu.snu.mist.client.rulebased.conditions.AbstractCondition;

/**
 * An immutable StatelessRule which should be used in stateless rule, which have condition and action.
 */
public class StatelessRule {

  /**
   * The condition.
   */
  protected final AbstractCondition condition;
  /**
   * The action.
   */
  protected final RuleBasedAction action;

  /**
   * Creates an immutable stateless rule.
   * @param condition
   * @param action
   */
  protected StatelessRule(final AbstractCondition condition, final RuleBasedAction action) {
    this.condition = condition;
    this.action = action;
  }

  /**
   * @return rule condition
   */
  public AbstractCondition getCondition() {
    return this.condition;
  }

  /**
   * @return rule action
   */
  public RuleBasedAction getAction() {
    return this.action;
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof StatelessRule)) {
      return false;
    }
    final StatelessRule rule = (StatelessRule) o;
    return this.condition.equals(rule.condition) && this.action.equals(rule.action);
  }

  @Override
  public int hashCode() {
    return condition.hashCode() * 10 + action.hashCode();
  }

  /**
   * Builder for StatelessRule.
   */
  public static final class Builder {

    private AbstractCondition condition;
    private RuleBasedAction action;

    /**
     * Create a new builder.
     */
    public Builder() {
      this.condition = null;
      this.action = null;
    }

    /**
     * Sets the condition.
     * @param condition condition
     * @return builder
     */
    public Builder setCondition(final AbstractCondition condition) {
      if (this.condition != null) {
        throw new IllegalStateException("Condition cannot be declared twice!");
      }
      this.condition = condition;
      return this;
    }

    /**
     * Sets the action.
     * @param action action
     * @return builder
     */
    public Builder setAction(final RuleBasedAction action) {
      if (this.action != null) {
        throw new IllegalStateException("Action cannot be declared twice!");
      }
      this.action = action;
      return this;
    }

    public StatelessRule build() {
      if (condition == null || action == null) {
        throw new IllegalStateException("Condition or action is not set!");
      }
      return new StatelessRule(condition, action);
    }
  }
}
