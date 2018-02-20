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
package edu.snu.mist.common.types;

/**
 * This interface defines basic Tuple interface. Tuple is the basic unit of stream
 * data representation with many fields in it. It has multiple dimensions (more than 2)
 * in it.
 *
 * Each tuple data is considered to be immutable, and users need to make new Tuples on
 * each transformation.
 */
public interface Tuple {

  // TODO[MIST-31]: Make tuples which have more dimensions than 3.
  /**
   * @return The number of fields in this Tuple.
   */
  int getDimension();

  /**
   * Gets the value of a specific field from Tuple.
   * @param field The field user wants to get value from
   * @return The value of the given field
   * @throws IllegalArgumentException raised when the field number is illegal
   */
  Object get(int field) throws IllegalArgumentException;

  /**
   * Gets the type of a specific field from Tuple. This method uses reflection, so don't call it unless it's necessary.
   * @param field The field user wants to get type from
   * @param <T> The returned type
   * @return The type of the given field
   * @throws IllegalArgumentException raised when the field number is illegal
   */
  <T> Class<T> getFieldType(int field) throws IllegalArgumentException;
}