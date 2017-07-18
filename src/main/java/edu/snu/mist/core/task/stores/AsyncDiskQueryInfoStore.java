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
package edu.snu.mist.core.task.stores;

import edu.snu.mist.core.parameters.TempFolderPath;
import edu.snu.mist.formats.avro.AvroOperatorChainDag;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * It saves the information of a query (operator chain dag, jar files) into the disk.
 * The store request is done by separated thread asynchronously.
 */
final class AsyncDiskQueryInfoStore implements QueryInfoStore {
  /**
   * A path for the temporary folder that stores jar files and operator chain dags of queries.
   */
  private final String tmpFolderPath;

  /**
   * A writer that stores the dag.
   */
  private final DatumWriter<AvroOperatorChainDag> datumWriter;

  /**
   * A reader that reads stored the dag.
   */
  private final DatumReader<AvroOperatorChainDag> datumReader;

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
  private final BlockingQueue<Tuple<String, AvroOperatorChainDag>> planQueue;

  /**
   * A set that contains the query ids that were stored properly.
   */
  private final Set<String> storedQuerySet;

  @Inject
  private AsyncDiskQueryInfoStore(@Parameter(TempFolderPath.class) final String tmpFolderPath,
                                  final FileNameGenerator fileNameGenerator) {
    this.tmpFolderPath = tmpFolderPath;
    this.fileNameGenerator = fileNameGenerator;
    this.datumWriter = new SpecificDatumWriter<>(AvroOperatorChainDag.class);
    this.datumReader = new SpecificDatumReader<>(AvroOperatorChainDag.class);
    // Create a folder that stores the dags and jar files
    final File folder = new File(tmpFolderPath);
    if (!folder.exists()) {
      folder.mkdir();
    } else {
      final File[] destroy = folder.listFiles();
      for (final File des : destroy) {
        des.delete();
      }
    }

    this.planQueue = new LinkedBlockingQueue<>();
    this.planStoreExecutorService = Executors.newSingleThreadExecutor();
    this.storedQuerySet = new ConcurrentSet<>();

    planStoreExecutorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          while (true) {
            final Tuple<String, AvroOperatorChainDag> tuple = planQueue.take();
            final String queryId = tuple.getKey();
            final AvroOperatorChainDag dag = tuple.getValue();
            final File storedFile = getAvroOperatorChainDagFile(queryId);
            if (!storedFile.exists()) {
              final DataFileWriter<AvroOperatorChainDag> dataFileWriter = new DataFileWriter<>(datumWriter);
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
  public void saveAvroOpChainDag(final Tuple<String, AvroOperatorChainDag> tuple) {
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
   * Saves the serialized jar files into the disk.
   * This generates file names for the jar files from fileNameGenerator.
   * @param jarFiles jar files
   * @return paths of the stored jar files
   * @throws IOException throws an exception when the jar file is not able to be saved.
   */
  @Override
  public List<String> saveJar(final List<ByteBuffer> jarFiles) throws IOException {
    final List<String> paths = new LinkedList<>();
    for (final ByteBuffer jarFileBytes : jarFiles) {
      final String path = String.format("submitted-%s.jar", fileNameGenerator.generate());
      final Path jarFilePath = Paths.get(tmpFolderPath, path);
      final File jarFile = jarFilePath.toFile();
      final FileChannel wChannel = new FileOutputStream(jarFile, false).getChannel();
      wChannel.write(jarFileBytes);
      wChannel.close();
      paths.add(jarFile.getAbsolutePath());
    }
    return paths;
  }

  /**
   * Load the stored dag from File.
   * @param storedPlanFile file
   * @return chained dag
   * @throws IOException
   */
  private AvroOperatorChainDag loadFromFile(final File storedPlanFile) throws IOException {
    final DataFileReader<AvroOperatorChainDag> dataFileReader =
        new DataFileReader<AvroOperatorChainDag>(storedPlanFile, datumReader);
    AvroOperatorChainDag dag = null;
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
  public AvroOperatorChainDag load(final String queryId) throws IOException {
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
    final AvroOperatorChainDag logicalPlan = loadFromFile(storedFile);
    final List<String> paths = logicalPlan.getJarFilePaths();

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
