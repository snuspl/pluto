package edu.snu.mist.core.task.threadpool.threadbased;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by taegeonum on 10/7/17.
 */
public final class QueryProgress {

  public final AtomicLong nextEventNum;
  public final AtomicLong eventNum;

  public QueryProgress() {
    this.nextEventNum = new AtomicLong(0);
    this.eventNum = new AtomicLong(0);
  }
}
