/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.mist.utils;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * This is an utility class for serialization/deserialization of avro object.
 */
public final class AvroSerializer {

  private AvroSerializer() {
    // empty
  }

  /**
   * Serialize avro object to string.
   * @param avroObject avro object
   * @param avroObjectClass object class
   * @param <T> object type
   * @return string
   */
  public static <T extends SpecificRecord> String avroToString(final T avroObject, final Class<T> avroObjectClass) {
    final DatumWriter<T> datumWriter = new SpecificDatumWriter<>(avroObjectClass);
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(avroObject.getSchema(), out);
      datumWriter.write(avroObject, encoder);
      encoder.flush();
      out.close();
      return out.toString();
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to serialize " + avroObjectClass.getName(), ex);
    }
  }

  /**
   * Deserialize string to avro object.
   * @param serializedObject string
   * @param schema avro schema
   * @param avroObjectClass object class
   * @param <T> object type
   * @return avro object
   */
  public static <T extends SpecificRecord> T avroFromString(final String serializedObject,
                                                            final Schema schema,
                                                            final Class<T> avroObjectClass) {
    try {
      final Decoder decoder =
          DecoderFactory.get().jsonDecoder(schema, serializedObject);
      final SpecificDatumReader<T> reader = new SpecificDatumReader<>(avroObjectClass);
      return reader.read(null, decoder);
    } catch (final IOException ex) {
      throw new RuntimeException("Unable to deserialize logical plan", ex);
    }
  }
}