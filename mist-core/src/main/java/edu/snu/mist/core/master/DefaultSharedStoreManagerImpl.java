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
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;

/**
 * The default shared store manager implementation class.
 */
public final class DefaultSharedStoreManagerImpl implements SharedStoreManager {

  /**
   * The shared store path.
   */
  private final String sharedStorePath;

  @Inject
  private DefaultSharedStoreManagerImpl(@Parameter(SharedStorePath.class) final String sharedStorePath) {
    this.sharedStorePath = sharedStorePath;
  }

  @Override
  public boolean clearSharedStore() {
    final File sharedStoreDir = new File(sharedStorePath);
    final File[] listing = sharedStoreDir.listFiles();
    if (listing == null) {
      throw new RuntimeException("Inavlid shared store directory path. Terminate MIST...");
    }
    if (listing.length == 0) {
      return true;
    }
    for (final File file: listing) {
      final String fileName = file.getName();
      if (fileName.endsWith(".checkpoint") || fileName.endsWith(".query") || fileName.endsWith(".querylist")) {
        // Try to delete the already existing query and checkpoint files...
        if (!file.delete()) {
          // If we fail to delete the existing files, there should be problems with the shared store...
          return false;
        }
      }
    }
    return true;
  }
}
