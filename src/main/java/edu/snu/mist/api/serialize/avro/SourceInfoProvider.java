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
package edu.snu.mist.api.serialize.avro;

import edu.snu.mist.api.sources.SourceStream;
import edu.snu.mist.formats.avro.SourceInfo;
import org.apache.reef.tang.annotations.DefaultImplementation;

/**
 * This is an interface for getting avro-serialized SourceInfo from MIST API SourceStream.
 */
@DefaultImplementation(SourceInfoProviderImpl.class)
public interface SourceInfoProvider {

  /**
   * @param sourceStream SourceStream defined via MIST API.
   * @return avro-serialized SourceInfo.
   */
  SourceInfo getSourceInfo(SourceStream sourceStream);
}
