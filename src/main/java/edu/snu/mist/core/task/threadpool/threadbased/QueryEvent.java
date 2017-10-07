package edu.snu.mist.core.task.threadpool.threadbased;

import edu.snu.mist.common.MistEvent;
import edu.snu.mist.common.graph.MISTEdge;
import edu.snu.mist.core.task.ExecutionVertex;

import java.util.Map;

/**
 * Created by taegeonum on 10/7/17.
 */
public final class QueryEvent {

  public final MistEvent event;
  public final Map<ExecutionVertex, MISTEdge> nextOperators;
  public final Object lockObject;

  public QueryEvent(final MistEvent event,
                    final Map<ExecutionVertex, MISTEdge> nextOperators,
                    final Object lockObject) {
    this.event =event;
    this.nextOperators = nextOperators;
    this.lockObject = lockObject;
  }
}
