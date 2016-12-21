/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.api.serialize.utils;

import org.apache.commons.lang.SerializationUtils;

import java.nio.ByteBuffer;

/**
 * This is a utility class for serialization test.
 */
public final class SerializationTestUtils {

  private SerializationTestUtils() {
    // empty constructor
  }

  /**
   * This method deserializes a serialized Object in a form of ByteBuffer.
   * @param bufferedObject the serialized Object
   * @param <T> the type of deserialized Object
   * @return deserialized Object
   */
  public static <T> T deserializeByteBuffer(final ByteBuffer bufferedObject) throws ClassCastException {
    final byte[] serializedObject = new byte[bufferedObject.remaining()];
    bufferedObject.get(serializedObject);
    return (T) SerializationUtils.deserialize(serializedObject);
  }
}
