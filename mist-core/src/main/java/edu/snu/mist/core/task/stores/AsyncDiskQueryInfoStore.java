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
package edu.snu.mist.core.task.stores;

import edu.snu.mist.core.parameters.SharedStorePath;
import edu.snu.mist.core.task.groupaware.ApplicationMap;
import edu.snu.mist.formats.avro.AvroDag;
import io.netty.util.internal.ConcurrentSet;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.reef.io.Tuple;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It saves the information of a query (operator chain dag, jar files) into the disk.
 * The store request is done by separated thread asynchronously.
 */
final class AsyncDiskQueryInfoStore implements QueryInfoStore {

  private static final Logger LOG = Logger.getLogger(QueryInfoStore.class.getName());
  /**
   * A path for the temporary folder that stores jar files and operator chain dags of queries.
   */
  private final String tmpFolderPath;

  /**
   * A writer that stores the dag.
   */
  private final DatumWriter<AvroDag> datumWriter;

  /**
   * A reader that reads stored the dag.
   */
  private final DatumReader<AvroDag> datumReader;

  /**
   * A file name generator that generates jar file's names.
   */
  private final FileNameGenerator fileNameGenerator;

  /**
   * A executor service that contains a thread doing plan store.
   */
  private final ExecutorService planStoreExecutorService;

  /**
   * A blocking queue that contains the plans to be stored.
   */
  private final BlockingQueue<Tuple<String, AvroDag>> planQueue;

  /**
   * A set that contains the query ids that were stored properly.
   */
  private final Set<String> storedQuerySet;

  /**
   * A map that contains the SHA-256 hash value (which is a byte array) converted to a ByteBuffer as the key,
   * and a list of tuples that correspond to that SHA-256 hash value.
   * A tuple is consisted of the ByteBuffer(of a jar file) and location(as a string) as a value for submitted jar files.
   * Multiple tuples may correspond to a single SHA-256 hash value, in case of hash collisions.
   * The key is a ByteBuffer because a byte array cannot be used as a key without a separate wrapper.
   */
  private final Map<ByteBuffer, List<Tuple<ByteBuffer, String>>> hashInfoMap;

  private final ApplicationMap metaApplicationMap;

  @Inject
  private AsyncDiskQueryInfoStore(@Parameter(SharedStorePath.class) final String tmpFolderPath,
                                  final ApplicationMap metaApplicationMap,
                                  final FileNameGenerator fileNameGenerator) {
    this.tmpFolderPath = tmpFolderPath;
    this.fileNameGenerator = fileNameGenerator;
    this.datumWriter = new SpecificDatumWriter<>(AvroDag.class);
    this.datumReader = new SpecificDatumReader<>(AvroDag.class);
    this.hashInfoMap = new ConcurrentHashMap<>();
    this.metaApplicationMap = metaApplicationMap;
    this.planQueue = new LinkedBlockingQueue<>();
    this.planStoreExecutorService = Executors.newSingleThreadExecutor();
    this.storedQuerySet = new ConcurrentSet<>();

    planStoreExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          while (true) {
            final Tuple<String, AvroDag> tuple = planQueue.take();
            final String queryId = tuple.getKey();
            final AvroDag dag = tuple.getValue();
            final File storedFile = getAvroOperatorChainDagFile(queryId);
            if (!storedFile.exists()) {
              final DataFileWriter<AvroDag> dataFileWriter = new DataFileWriter<>(datumWriter);
              dataFileWriter.create(dag.getSchema(), storedFile);
              dataFileWriter.append(dag);
              dataFileWriter.close();
              storedQuerySet.add(queryId);
            }
          }
        } catch (final Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  /**
   * Gets a file of the stored the dag corresponding to the queryId.
   * @param queryId query id
   * @return file of the stored dag
   */
  private File getAvroOperatorChainDagFile(final String queryId) {
    final StringBuilder sb = new StringBuilder(queryId);
    sb.append(".plan");
    return new File(tmpFolderPath, sb.toString());
  }

  /**
   * Saves the dag as queryId.plan to disk asynchronously.
   * @param tuple the tuple to save
   */
  @Override
  public void saveAvroDag(final Tuple<String, AvroDag> tuple) {
    try {
      planQueue.put(tuple);
    } catch (final InterruptedException ie) {
      ie.printStackTrace();
    }
  }

  /**
   * Check whether the query is stored properly or not.
   * @param queryId the query id to check
   * @return true if saving is success. Otherwise return false
   */
  @Override
  public boolean isStored(final String queryId) {
    return storedQuerySet.contains(queryId);
  }

  /**
   * Check for hash collisions within the jarInfoList and add the path(s) if the same jar was previously submitted.
   * @param jarFileBytes
   * @param jarInfoList
   * @param paths
   * @return true if the same jar was previously submitted.
   */
  private boolean checkSameJar(final ByteBuffer jarFileBytes,
                               final List<Tuple<ByteBuffer, String>> jarInfoList,
                               final List<String> paths) {
    // The following boolean is used to see if a hash collision has occurred.
    boolean sameJarExists = false;
    // Check if any of the actual ByteBuffers are also the same.
    for (final Tuple<ByteBuffer, String> jarInfo : jarInfoList) {
      if (Arrays.equals(jarFileBytes.array(), jarInfo.getKey().array())) {
        LOG.log(Level.INFO, "The jar file submitted was already previously submitted.");
        final String path = jarInfo.getValue();
        paths.add(path);
        sameJarExists = true;
        break;
      }
    }
    return sameJarExists;
  }

  /**
   * Load the stored dag from File.
   * @param storedPlanFile file
   * @return chained dag
   * @throws IOException
   */
  private AvroDag loadFromFile(final File storedPlanFile) throws IOException {
    final DataFileReader<AvroDag> dataFileReader =
        new DataFileReader<AvroDag>(storedPlanFile, datumReader);
    AvroDag dag = null;
    dag = dataFileReader.next(dag);
    return dag;
  }

  /**
   * Loads the dag, queryId.plan, from disk.
   * @param queryId
   * @return the dag corresponding to queryId
   * @throws IOException
   */
  @Override
  public AvroDag load(final String queryId) throws IOException {
    final File storedFile = getAvroOperatorChainDagFile(queryId);
    return loadFromFile(storedFile);
  }

  /**
   * Deletes the dag and jar files.
   * @param queryId
   * @throws IOException
   */
  @Override
  public void delete(final String queryId) throws IOException {
    storedQuerySet.remove(queryId);
    final File storedFile = getAvroOperatorChainDagFile(queryId);
    final AvroDag logicalPlan = loadFromFile(storedFile);
    final String appId = logicalPlan.getAppId();
    final List<String> paths = metaApplicationMap.get(appId).getJarFilePath();

    // Delete jar files for the query
    for (final String path : paths) {
      if (path.startsWith(tmpFolderPath)) {
        // Delete the jar file if it is in the temp folder
        final File jarFile = new File(path);
        if (jarFile.exists()) {
          jarFile.delete();
        }
      }
    }

    // Delete the logical plan
    if (storedFile.exists()) {
      storedFile.delete();
    }
  }

  @Override
  public void close() {
    planStoreExecutorService.shutdown();
  }
}
