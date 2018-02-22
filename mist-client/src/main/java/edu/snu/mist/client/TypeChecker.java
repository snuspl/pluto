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

package edu.snu.mist.client;

import edu.snu.mist.client.datastreams.MISTStream;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * This utility class represents the type checker that checking streams' types dynamically.
 */
public final class TypeChecker {

  private TypeChecker() {
    // won't be called
  }

  /**
   * Checks whether these two continuous streams have same type or not.
   * @param stream1 the first stream to check type
   * @param stream2 the second stream to check type
   * @param <T1> the data type of first stream
   * @param <T2> the data type of second stream
   * @return type checking result whether they match or not
   */
  public static <T1, T2> boolean checkTypesEqual(final MISTStream<T1> stream1, final MISTStream<T2> stream2) {
    // TODO[MIST-245]: Improve type checking. Type checking in below checks only generic type's name.
    // It can return false even though two types are equal, so we disable it (MIST-286)
    final Type[] types1 = getStreamType(stream1);
    final Type[] types2 = getStreamType(stream2);

    if (types1.length != types2.length) {
      return false;
    }

    for (int i = 0; i < types1.length; i++) {
      if (!types1[i].equals(types2[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Private function get reflect type of stream.
   */
  private static <T> Type[] getStreamType(final MISTStream<T> stream) {
    return ((ParameterizedType)stream.getClass().getGenericSuperclass()).getActualTypeArguments();
  }
}
