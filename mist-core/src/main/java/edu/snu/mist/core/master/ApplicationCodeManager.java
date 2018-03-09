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
package edu.snu.mist.core.master;

import edu.snu.mist.formats.avro.JarUploadResult;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * The interface for classes which manage submitted application codes.
 */
@DefaultImplementation(DefaultApplicationCodeManager.class)
public interface ApplicationCodeManager {

  /**
   * Registers the new query code with a given Jar file.
   * @return The path for the saved
   */
  JarUploadResult registerNewAppCode(List<ByteBuffer> jarFiles);

  /**
   * Returns the jar paths for a given application id.
   * @param appId
   * @return the list of jar paths
   */
  List<String> getJarPaths(String appId);
}
