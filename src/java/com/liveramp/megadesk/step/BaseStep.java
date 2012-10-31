package com.liveramp.megadesk.step;

import com.liveramp.megadesk.resource.Resource;
import org.apache.log4j.Logger;

import java.util.List;

public abstract class BaseStep implements Step {

  private static final Logger LOGGER = Logger.getLogger(BaseStep.class);

  private String id;
  private final List<Resource> reads;
  private final List<Resource> writes;

  public BaseStep(String id,
                  List<Resource> reads,
                  List<Resource> writes) {
    this.id = id;
    this.reads = reads;
    this.writes = writes;
  }

  public String getId() {
    return id;
  }

  public abstract StepLock getStepLock();

  @Override
  public List<Resource> getReads() {
    return reads;
  }

  @Override
  public List<Resource> getWrites() {
    return writes;
  }

  public void attempt() throws Exception {
    LOGGER.info("Attempting step " + getId());
    // Acquire all locks in order
    // TODO: potential dead locks
    getStepLock().acquire();
    for (Resource read : reads) {
      LOGGER.info("Acquiring read resource " + read.getId());
      read.getReadLock().acquire(this);
    }
    for (Resource write : writes) {
      LOGGER.info("Acquiring write resource " + write.getId());
      write.getWriteLock().acquire(this);
    }
  }

  public void complete() throws Exception {
    LOGGER.info("Completing step " + getId());
    // Make sure this process is allowed to complete this step
    if (!getStepLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot complete step " + getId() + " that is not being attempted in this process.");
    }
    // Release all locks in order
    for (Resource read : reads) {
      LOGGER.info("Releasing read resource " + read.getId());
      read.getReadLock().release(this);
    }
    for (Resource write : writes) {
      LOGGER.info("Releasing write resource " + write.getId());
      write.getWriteLock().release(this);
    }
    getStepLock().release();
  }
}
