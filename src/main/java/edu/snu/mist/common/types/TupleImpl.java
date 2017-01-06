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
package edu.snu.mist.common.types;

import java.util.List;

/**
 * This class contains the implementation of common methods necessary for Tuples.
 */
public abstract class TupleImpl implements Tuple {

  /**
   * The number of fields for this tuple.
   */
  private final int numFields;
  /**
   * The List for storing actual data.
   */
  private final List<Object> dataList;

  TupleImpl(final List<Object> dataList) {
    if (dataList.isEmpty()) {
      throw new IllegalArgumentException("Cannot create empty Tuple!");
    }
    this.dataList = dataList;
    this.numFields = dataList.size();
  }

  @Override
  public final int getDimension() {
    return numFields;
  }

  @Override
  public final Object get(final int field) {
    if (field < 0 || field >= dataList.size()) {
      throw new IllegalArgumentException("The field number is not in the Tuple range!");
    } else {
      return dataList.get(field);
    }
  }

  @Override
  public final <T> Class<T> getFieldType(final int field) {
    if (field < 0 || field >= dataList.size()) {
      throw new IllegalArgumentException("The field number is not in the Tuple range!");
    } else {
      return (Class<T>)dataList.get(field).getClass();
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (!(o instanceof TupleImpl)) {
      return false;
    } else {
      return ((TupleImpl) o).dataList.equals(this.dataList);
    }
  }

  @Override
  public int hashCode() {
    return dataList.hashCode();
  }
}
