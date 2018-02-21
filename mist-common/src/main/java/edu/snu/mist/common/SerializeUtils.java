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
package edu.snu.mist.common;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

/**
 * A utility class for serialization and deserialization of objects.
 */
public final class SerializeUtils {

  private SerializeUtils() {
    // do nothing
  }

  /**
   * Read the object from Base64 string.
   * @param s serialized object
   * @param <T> object type
   * @return object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static <T> T deserializeFromString(
      final String s) throws IOException, ClassNotFoundException {
    final byte[] data = Base64.getDecoder().decode(s);
    final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
    final T object  = (T)ois.readObject();
    ois.close();
    return object;
  }

  /**
   * Read the object from Base64 string with the external class loader.
   * @param s serialized object
   * @param classLoader an external class loader
   * @param <T> object type
   * @return object
   * @throws IOException
   * @throws ClassNotFoundException
   */
  public static <T> T deserializeFromString(
      final String s,
      final ClassLoader classLoader) throws IOException, ClassNotFoundException {
    final byte[] data = Base64.getDecoder().decode(s);
    final ExternalJarObjectInputStream stream = new ExternalJarObjectInputStream(
        classLoader, data);
    final T object  = (T)stream.readObject();
    stream.close();
    return object;
  }

  /**
   * Write the object to a Base64 string.
   * @param obj object
   * @return serialized object
   * @throws IOException
   */
  public static String serializeToString(final Serializable obj) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    oos.close();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

  /**
   * Get urls from the list of String.
   * @param paths list of string
   * @return url array
   **/
  public static URL[] getJarFileURLs(final List<String> paths) throws MalformedURLException {
    final URL[] urls = new URL[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      final String jarFilePath = paths.get(i);
      final URL url = Paths.get(jarFilePath).toUri().toURL();
      urls[i] = url;
    }
    return urls;
  }
}
