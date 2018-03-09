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
import edu.snu.mist.formats.avro.AvroPhysicalOperatorChain;
import edu.snu.mist.formats.avro.AvroPhysicalSourceOutgoingEdgesInfo;
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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class saves AvroPhysicalOperatorChains, which must be added to the store in their "built" state.
 * It is used for activating and deactivating sources.
 */
public final class AvroExecutionVertexStore {

  private static final Logger LOG = Logger.getLogger(AvroExecutionVertexStore.class.getName());

  private final String tmpFolderPath;

  /**
   * A writer that stores AvroPhysicalOperatorChains.
   */
  private final DatumWriter<AvroPhysicalOperatorChain> operatorChainDatumWriter;

  /**
   * A reader that reads stored AvroPhysicalOperatorChains.
   */
  private final DatumReader<AvroPhysicalOperatorChain> operatorChainDatumReader;

  /**
   * A writer that stores AvroPhysicalSourceOutgoingEdgesInfos.
   */
  private final DatumWriter<AvroPhysicalSourceOutgoingEdgesInfo> sourceDatumWriter;

  /**
   * A reader that reads stored AvroPhysicalSourceOutgoingEdgesInfos.
   */
  private final DatumReader<AvroPhysicalSourceOutgoingEdgesInfo> sourceDatumReader;

  /**
   * A file name generator that generates AvroPhysicalOperatorChain file's names.
   */
  private final FileNameGenerator fileNameGenerator;

  @Inject
  private AvroExecutionVertexStore(@Parameter(SharedStorePath.class) final String tmpFolderPath,
                                   final FileNameGenerator fileNameGenerator) {
    this.tmpFolderPath = tmpFolderPath;
    this.fileNameGenerator = fileNameGenerator;
    this.operatorChainDatumWriter = new SpecificDatumWriter<>(AvroPhysicalOperatorChain.class);
    this.operatorChainDatumReader = new SpecificDatumReader<>(AvroPhysicalOperatorChain.class);
    this.sourceDatumWriter = new SpecificDatumWriter<>(AvroPhysicalSourceOutgoingEdgesInfo.class);
    this.sourceDatumReader = new SpecificDatumReader<>(AvroPhysicalSourceOutgoingEdgesInfo.class);
  }

  /**
   * Gets a file of the stored operator chain corresponding to the operator chain id.
   */
  private File getAvroPhysicalOperatorChainFile(final String operatorChainId) {
    final StringBuilder sb = new StringBuilder(operatorChainId);
    sb.append(".chain");
    return new File(tmpFolderPath, sb.toString());
  }

  /**
   * Saves the AvroPhysicalOperatorChain as operatorChainId.chain to disk.
   */
  public void saveAvroPhysicalOperatorChain(final Tuple<String, AvroPhysicalOperatorChain> tuple) {
    try {
      final AvroPhysicalOperatorChain avroPhysicalOperatorChain = tuple.getValue();
      // Create file with the name of the PhysicalOperatorChain Id.
      final File avroPhysicalOperatorChainFile = getAvroPhysicalOperatorChainFile(tuple.getKey());
      final DataFileWriter<AvroPhysicalOperatorChain> dataFileWriter
          = new DataFileWriter<>(operatorChainDatumWriter);
      dataFileWriter.create(avroPhysicalOperatorChain.getSchema(), avroPhysicalOperatorChainFile);
      dataFileWriter.append(avroPhysicalOperatorChain);
      dataFileWriter.close();
    } catch (IOException e) {
      throw new RuntimeException("Writing AvroPhysicalOperatorChain has failed.");
    }
  }

  /**
   * Loads the AvroPhysicalOperatorChain with the chainId.
   */
  public AvroPhysicalOperatorChain loadAvroPhysicalOperatorChain(final String chainId) throws IOException {
    try {
      final File storedChain = getAvroPhysicalOperatorChainFile(chainId);
      final DataFileReader<AvroPhysicalOperatorChain> dataFileReader =
          new DataFileReader<>(storedChain, operatorChainDatumReader);
      AvroPhysicalOperatorChain avroPhysicalOperatorChain = null;
      avroPhysicalOperatorChain = dataFileReader.next(avroPhysicalOperatorChain);
      return avroPhysicalOperatorChain;
    } catch (final IOException e) {
      LOG.log(Level.SEVERE, "An exception occurred while loading the AvroPhysicalOperatorChain with ID {0}.",
          new Object[] {chainId});
      throw e;
    }
  }

  /**
   * Gets a file of the stored source corresponding to the source id.
   */
  private File getAvroPhysicalSourceOutgoingEdgesInfoFile(final String sourceId) {
    final StringBuilder sb = new StringBuilder(sourceId);
    sb.append(".source");
    return new File(tmpFolderPath, sb.toString());
  }

  /**
   * Loads the AvroPhysicalSourceOutgoingEdgesInfo of the source to be reactivated.
   */
  public AvroPhysicalSourceOutgoingEdgesInfo loadAvroPhysicalSourceOutgoingEdgesInfo(final String sourceId)
      throws IOException {
    final File storedSourceFile = getAvroPhysicalSourceOutgoingEdgesInfoFile(sourceId);
    return loadAvroPhysicalSourceOutgoingEdgesInfoFromFile(storedSourceFile);
  }

  // TODO: [MIST-*] Implement policy for deleting from disk. Currently does not delete from disk.
  private AvroPhysicalSourceOutgoingEdgesInfo loadAvroPhysicalSourceOutgoingEdgesInfoFromFile(final File storedSource) {
    try {
      final DataFileReader<AvroPhysicalSourceOutgoingEdgesInfo> dataFileReader =
          new DataFileReader<>(storedSource, sourceDatumReader);
      AvroPhysicalSourceOutgoingEdgesInfo avroPhysicalSourceOutgoingEdgesInfo = null;
      avroPhysicalSourceOutgoingEdgesInfo = dataFileReader.next(avroPhysicalSourceOutgoingEdgesInfo);
      return avroPhysicalSourceOutgoingEdgesInfo;
    } catch (IOException e) {
      throw new RuntimeException("Loading AvroPhysicalSourceOutgoingEdgesInfo has failed.");
    }
  }

  /**
   * Saves the AvroPhysicalSourceOutgoingEdgesInfo of the source to be reactivated.
   */
  public void saveAvroPhysicalSourceOutgoingEdgesInfo(final Tuple<String, AvroPhysicalSourceOutgoingEdgesInfo> tuple) {
    try {
      final AvroPhysicalSourceOutgoingEdgesInfo avroPhysicalSourceOutgoingEdgesInfo = tuple.getValue();
      // Create file with the name of the PhysicalOperatorChain Id.
      final File avroPhysicalSourceOutgoingEdgesInfoFile = getAvroPhysicalSourceOutgoingEdgesInfoFile(tuple.getKey());
      final DataFileWriter<AvroPhysicalSourceOutgoingEdgesInfo> dataFileWriter
          = new DataFileWriter<>(sourceDatumWriter);
      dataFileWriter.create(avroPhysicalSourceOutgoingEdgesInfo.getSchema(), avroPhysicalSourceOutgoingEdgesInfoFile);
      dataFileWriter.append(avroPhysicalSourceOutgoingEdgesInfo);
      dataFileWriter.close();
    } catch (IOException e) {
      throw new RuntimeException("Writing AvroPhysicalSourceOutgoingEdgesInfo has failed.");
    }
  }
}