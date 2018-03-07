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

import edu.snu.mist.formats.avro.JarUploadResult;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public final class MistClient {

  private MistClient() {

  }

  public static void main(final String[] args) throws IOException {
    if (args.length < 5) {
      System.out.println("Please enter the command-line parameters: ");
      System.out.println("submit-jar $jar_path$ $driver_address$ $driver_port$ $app_name");
      return;
    }

    final String type = args[0];
    if (type.compareToIgnoreCase("submit-jar") == 0) {
      final String jarPath = args[1];
      final File file = new File(jarPath);

      if (!file.exists()) {
        System.out.println("File " + jarPath + " does not exist");
        return;
      }

      final String address = args[2];
      final int port = Integer.valueOf(args[3]);
      final String appName = args[4];

      try (final MISTExecutionEnvironment ee = new MISTDefaultExecutionEnvironmentImpl(address, port, appName)) {
        final JarUploadResult result = ee.submitJar(Arrays.asList(jarPath));
        if (result.getIsSuccess()) {
          System.out.println("App identifier: " + result.getIdentifier());
          return;
        } else {
          System.out.println("Submission failed: " + result.getMsg());
        }
      } catch (final Exception e) {
        e.printStackTrace();
      }
    } else {
      System.out.println("Not supported operation");
      return;
    }
  }
}
