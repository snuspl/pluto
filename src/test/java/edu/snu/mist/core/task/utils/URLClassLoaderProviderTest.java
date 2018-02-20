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
package edu.snu.mist.core.task.utils;

import edu.snu.mist.core.task.codeshare.ClassLoaderProvider;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;

public final class URLClassLoaderProviderTest {

  @Test
  public void classLoaderSharingTest() throws InjectionException, MalformedURLException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    final ClassLoaderProvider classLoaderProvider = injector.getInstance(ClassLoaderProvider.class);

    final List<String> paths = Arrays.asList();
    final ClassLoader classLoader1 = classLoaderProvider.newInstance(paths);
    final ClassLoader classLoader2 = classLoaderProvider.newInstance(paths);

    Assert.assertTrue(classLoader1 == classLoader2);
  }
}
