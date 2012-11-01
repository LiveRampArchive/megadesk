package com.liveramp.megadesk.step;

import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import org.apache.log4j.Logger;

import java.util.List;

public abstract class BaseStep implements Step {

  private static final Logger LOGGER = Logger.getLogger(BaseStep.class);

  private String id;
  private final List<Read> reads;
  private final List<Resource> writes;

  public BaseStep(String id,
                  List<Read> reads,
                  List<Resource> writes) {
    this.id = id;
    this.reads = reads;
    this.writes = writes;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public List<Read> getReads() {
    return reads;
  }

  @Override
  public List<Resource> getWrites() {
    return writes;
  }

  @Override
  public void attempt() throws Exception {
    LOGGER.info("Attempting step '" + getId() + "'");
    // Acquire all locks in order
    // TODO: potential dead locks
    getLock().acquire();
    for (Read read : reads) {
      read.getResource().getReadLock().acquire(getId(), read.getStateCheck(), true);
    }
    for (Resource write : writes) {
      write.getWriteLock().acquire(getId(), true);
    }
  }

  @Override
  public void complete() throws Exception {
    LOGGER.info("Completing step '" + getId() + "'");
    // Make sure this process is allowed to complete this step
    if (!getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot complete step '" + getId() + "' that is not being attempted in this process.");
    }
    // Release all locks in order
    for (Read read : reads) {
      read.getResource().getReadLock().release(getId());
    }
    for (Resource write : writes) {
      write.getWriteLock().release(getId());
    }
    getLock().release();
  }

  @Override
  public void setState(Resource resource, Object state) throws Exception {
    if (!getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot set state of resource '" + resource.getId() + "' from step '" + getId() + "' that is not being attempted in this process.");
    }
    resource.setState(getId(), state);
  }

  protected abstract StepLock getLock();
}
