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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * This class is used for reading objects from external JARs.
 */
public final class ExternalJarObjectInputStream extends ObjectInputStream {

  private ClassLoader classLoader;

  public ExternalJarObjectInputStream(final ClassLoader classLoader, final byte[] bytes) throws IOException {
    super(new ByteArrayInputStream(bytes));
    this.classLoader = classLoader;
  }

  @Override
  protected Class<?> resolveClass(final ObjectStreamClass desc) throws ClassNotFoundException {
    String name = desc.getName();
    try {
      return Class.forName(name, false, classLoader);
    } catch (ClassNotFoundException ex) {
      throw ex;
    }
  }
}