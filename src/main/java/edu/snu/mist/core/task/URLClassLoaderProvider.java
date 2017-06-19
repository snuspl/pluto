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
package edu.snu.mist.core.task;

import javax.inject.Inject;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * URL class loader provider that shares jar files when the urls are the same.
 */
final class URLClassLoaderProvider implements ClassLoaderProvider {

  private final ConcurrentMap<List<URL>, ClassLoader> classLoaderConcurrentMap;

  @Inject
  private URLClassLoaderProvider() {
    this.classLoaderConcurrentMap = new ConcurrentHashMap<>();
  }

  @Override
  public ClassLoader newInstance(final URL[] urls) {
    final List<URL> urlList = new ArrayList<>(urls.length);
    for (int i = 0; i < urls.length; i++) {
      urlList.add(urls[i]);
    }

    if (classLoaderConcurrentMap.get(urlList) == null) {
      classLoaderConcurrentMap.putIfAbsent(urlList, new URLClassLoader(urls));
      return classLoaderConcurrentMap.get(urlList);
    } else {
      return classLoaderConcurrentMap.get(urlList);
    }
  }

  @Override
  public ClassLoader newInstance(final List<String> paths) throws MalformedURLException {
    // Get jar files' urls
    final URL[] urls = new URL[paths.size()];
    for (int i = 0; i < paths.size(); i++) {
      final String jarFilePath = paths.get(i);
      final URL url = new URL(jarFilePath);
      urls[i] = url;
    }
    return newInstance(urls);
  }
}