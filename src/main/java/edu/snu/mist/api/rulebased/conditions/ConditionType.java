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
package edu.snu.mist.api.rulebased.conditions;

/**
 * Enum class for condition types.
 */

public enum ConditionType {
  // Less than
  LT,
  // Less than or equal to
  LE,
  // Greater than
  GT,
  // Greater than or equal to
  GE,
  // Equals
  EQ,
  // Not equals
  NEQ,
  // And
  AND,
  // Or
  OR
}