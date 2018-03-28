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
package edu.snu.mist.core.task;

import edu.snu.mist.core.parameters.QueryIdPrefix;
import edu.snu.mist.formats.avro.AvroDag;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

public final class QueryIdGeneratorTest {

  /**
   * Test whether QueryIdGenerator generates query ids correctly.
   * It creates 10,000 query ids and checks the ids.
   * @throws org.apache.reef.tang.exceptions.InjectionException
   */
  @Test
  public void testQueryIdGenerate() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final QueryIdGenerator queryIdGenerator = injector.getInstance(QueryIdGenerator.class);
    final String prefix = injector.getNamedInstance(QueryIdPrefix.class);
    final AvroDag avroDag = new AvroDag();
    long submittedQueryNum = 0;
    while (submittedQueryNum < 10000) {
      final String queryId = queryIdGenerator.generate();
      Assert.assertEquals(prefix + submittedQueryNum, queryId);
      submittedQueryNum++;
    }
  }
}
