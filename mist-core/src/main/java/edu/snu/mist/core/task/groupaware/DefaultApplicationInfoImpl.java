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
package edu.snu.mist.core.task.groupaware;

import edu.snu.mist.core.task.QueryRemover;
import edu.snu.mist.core.task.QueryStarter;
import edu.snu.mist.core.task.groupaware.parameters.ApplicationIdentifier;
import edu.snu.mist.core.task.groupaware.parameters.JarFilePath;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public final class DefaultApplicationInfoImpl implements ApplicationInfo {

  private static final Logger LOG = Logger.getLogger(DefaultApplicationInfoImpl.class.getName());

  private final List<Group> groups;

  private final AtomicInteger numGroups = new AtomicInteger(0);

  /**
   * The jar file path.
   */
  private final List<String> jarFilePath;

  /**
   * The application identifier.
   */
  private final String appId;

  /**
   * A query starter.
   */
  private final QueryStarter queryStarter;

  /**
   * Query remover that deletes queries.
   */
  private final QueryRemover queryRemover;

  @Inject
  private DefaultApplicationInfoImpl(@Parameter(ApplicationIdentifier.class) final String appId,
                                     @Parameter(JarFilePath.class) final String jarFilePath,
                                     final QueryStarter queryStarter,
                                     final QueryRemover queryRemover) {
    this.groups = new LinkedList<>();
    this.jarFilePath = Arrays.asList(jarFilePath);
    this.appId = appId;
    this.queryStarter = queryStarter;
    this.queryRemover = queryRemover;
  }

  @Override
  public List<Group> getGroups() {
    return groups;
  }

  @Override
  public Group getRandomGroup() {
    final Random random = new Random();
    return groups.get(random.nextInt(groups.size()));
  }

  @Override
  public boolean addGroup(final Group group) {
    group.setApplicationInfo(this);
    numGroups.incrementAndGet();
    return groups.add(group);
  }

  @Override
  public AtomicInteger numGroups() {
    return numGroups;
  }

  @Override
  public String getApplicationId() {
    return appId;
  }

  @Override
  public List<String> getJarFilePath() {
    return jarFilePath;
  }

  @Override
  public QueryStarter getQueryStarter() {
    return queryStarter;
  }

  @Override
  public QueryRemover getQueryRemover() {
    return queryRemover;
  }
}