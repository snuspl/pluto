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
package edu.snu.mist.core.task;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * This class provides static methods concerning checks for jar duplication.
 * that converts a ByteBuffer to a SHA-256 hash, which is a byte array.
 * This class is stateless.
 */
public class HashUtils {

  // Converts a ByteBuffer to a SHA-256 hash.
  public static byte[] getByteBufferHash(final ByteBuffer byteBuffer) {
    try {
      final MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] byteBufferArray = new byte[byteBuffer.slice().remaining()];
      byteBuffer.get(byteBufferArray);
      return md.digest(byteBufferArray);
    } catch (final NoSuchAlgorithmException e) {
      e.printStackTrace();
      throw new RuntimeException("SHA-256 algorithm is not available in the current environment.");
    }
  }

  // Check if 2 ByteBuffers are the same.
  public static boolean byteBufferEquals(final ByteBuffer buffer1, ByteBuffer buffer2) {
    final byte[] f1 = getByteBufferHash(buffer1);
    final byte[] f2 = getByteBufferHash(buffer1);
    return Arrays.equals(f1, f2);
  }
}
