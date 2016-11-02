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
package edu.snu.mist.core.driver;

import edu.snu.mist.formats.avro.MistTaskProvider;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.reef.tang.ExternalConstructor;

import javax.inject.Inject;

/**
 * A wrapper class for Avro SpecificResponder.
 */
public final class SpecificResponderWrapper implements ExternalConstructor<SpecificResponder> {

  /**
   * A Specific responder.
   */
  private final SpecificResponder responder;

  @Inject
  private SpecificResponderWrapper(final TaskSelector taskSelector) {
    this.responder = new SpecificResponder(MistTaskProvider.class, taskSelector);
  }

  @Override
  public SpecificResponder newInstance() {
    return responder;
  }
}
