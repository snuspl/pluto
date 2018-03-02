package edu.snu.mist.core.master;

import javax.inject.Inject;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The injectable map which contains all the task information maintained by mist master.
 */
public class TaskInfoMap {

  private final ConcurrentMap<InetSocketAddress, TaskInfo> innerMap;

  @Inject
  private TaskInfoMap() {
    this.innerMap = new ConcurrentHashMap<>();
  }

  public TaskInfo addNewTaskInfo(final InetSocketAddress taskAddress, final TaskInfo taskInfo) {
    return innerMap.putIfAbsent(taskAddress, taskInfo);
  }

  public TaskInfo deleteTaskInfo(final InetSocketAddress taskAddress) {
    return innerMap.remove(taskAddress);
  }
}
