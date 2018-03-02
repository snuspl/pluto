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
import org.apache.avro.AvroRemoteException;

import java.io.IOException;
import java.util.List;

/**
 * Execution Environment for MIST.
 * MIST Client can submit queries via this class.
 */
public interface MISTExecutionEnvironment extends AutoCloseable {
  /**
   * Submit the query and its corresponding jar files to MIST.
   * @param queryToSubmit a query to be submitted.
   * @return the result of the query submission.
   * @throws IOException an exception occurs when connecting with MIST and serializing the jar files.
   */
  APIQueryControlResult submitQuery(MISTQuery queryToSubmit)  throws AvroRemoteException;

  /**
   * Submit jar files for the application.
   * It returns the identifier of the jar file and the client can submit multiple queries of the application.
   * @param jarFilePaths jar file paths
   * @return upload result
   * @throws IOException exception when the jar file does not exist
   */
  JarUploadResult submitJar(List<String> jarFilePaths) throws IOException;
}
