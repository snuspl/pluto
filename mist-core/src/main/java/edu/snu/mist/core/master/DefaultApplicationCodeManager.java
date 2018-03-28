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

import edu.snu.mist.core.parameters.SharedStorePath;
import edu.snu.mist.formats.avro.JarUploadResult;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The default implementation for application coide manager.
 */
public final class DefaultApplicationCodeManager implements ApplicationCodeManager {

  private static final Logger LOG = Logger.getLogger(DefaultApplicationCodeManager.class.getName());

  /**
   * The map for storing app ID and jar paths.
   */
  private final ConcurrentMap<String, List<String>> appJarMap;

  /**
   * The atomic integer for generating application identifier.
   */
  private final AtomicInteger appIdentifierNum;

  /**
   * The path to the directory where the application JAR files would be stored.
   */
  private final String jarStoringPath;

  @Inject
  private DefaultApplicationCodeManager(
      @Parameter(SharedStorePath.class) final String jarStoringPath
  ) {
    this.appJarMap = new ConcurrentHashMap<>();
    this.appIdentifierNum = new AtomicInteger(0);
    this.jarStoringPath = jarStoringPath;
  }

  /**
   * Create a JarFile with the given bytes and path, and also add it to the paths.
   * @param jarFileBytes
   * @param jarFilePath
   * @param paths
   * @throws IOException
   */
  private void createJarFile(final ByteBuffer jarFileBytes,
                             final Path jarFilePath,
                             final List<String> paths) throws IOException {

    final File file = new File(jarFilePath.toString());

    // Check whether the file is stored successfully.
    while (!file.exists()) {
      final File jarFile = jarFilePath.toFile();
      final FileChannel wChannel = new FileOutputStream(jarFile, false).getChannel();
      wChannel.write(jarFileBytes);
      wChannel.close();

      try {
        Thread.sleep(1000);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      }
    }

    paths.add(jarFilePath.toAbsolutePath().toString());
  }

  /**
   * Saves the serialized jar files into the disk.
   * @param jarFiles jar files
   * @param appId the given application id
   * @return paths of the stored jar files
   * @throws IOException throws an exception when the jar file is not able to be saved.
   */
  private List<String> saveJar(final List<ByteBuffer> jarFiles, final String appId) throws IOException {
    final List<String> paths = new LinkedList<>();
    for (final ByteBuffer jarFileBytes : jarFiles) {
      final String path = String.format("submitted-%s.jar", appId);
      final Path jarFilePath = Paths.get(jarStoringPath, path);
      createJarFile(jarFileBytes, jarFilePath, paths);
    }
    return paths;
  }

  @Override
  public JarUploadResult registerNewAppCode(final List<ByteBuffer> jarFiles) {
    try {
      // TODO: Compare code hash values to prevent duplicate app registration.
      final String appId = String.valueOf(appIdentifierNum.getAndIncrement());
      final List<String> jarPaths = saveJar(jarFiles, appId);
      // App ID is always unique, so putIfAbsent() isn't necessary.
      appJarMap.put(appId, jarPaths);
      return JarUploadResult.newBuilder()
          .setIdentifier(appId)
          .setIsSuccess(true)
          .setMsg("")
          .setJarPaths(jarPaths)
          .build();
    } catch (final IOException e) {
      LOG.log(Level.SEVERE, "I/O exception occurred during saving jar files. " + e.toString());
      return JarUploadResult.newBuilder()
          .setIdentifier("FAILED")
          .setIsSuccess(false)
          .setMsg("I/O Exception occured while saving jar files")
          .setJarPaths(new ArrayList<>())
          .build();
    }
  }

  @Override
  public List<String> getJarPaths(final String appId) {
    return appJarMap.get(appId);
  }

}