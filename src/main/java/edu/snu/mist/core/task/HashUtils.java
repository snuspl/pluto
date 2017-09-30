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

/**
 * This class provides static methods concerning checks for jar duplication.
 * that converts a ByteBuffer to a SHA-256 hash, which is a byte array.
 * This class is stateless.
 */
public final class HashUtils {

  private static MessageDigest md;

  private HashUtils() {
    // do nothing
  }

  static {
    md = null;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      e.printStackTrace();
      throw new RuntimeException("SHA-256 algorithm is not available in the current environment.");
    }
  }

  // Converts a ByteBuffer to a SHA-256 hash.
  public static byte[] getByteBufferHash(final ByteBuffer byteBuffer) {
    final ByteBuffer bufferCopy = byteBuffer.slice();
    final byte[] byteBufferArray = new byte[bufferCopy.remaining()];
    bufferCopy.get(byteBufferArray);
    return md.digest(byteBufferArray);
  }
}
