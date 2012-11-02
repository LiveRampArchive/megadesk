package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import org.apache.log4j.Logger;

import java.util.List;

public abstract class BaseManeuver implements Maneuver {

  private static final Logger LOGGER = Logger.getLogger(BaseManeuver.class);

  private String id;
  private final List<Read> reads;
  private final List<Resource> writes;

  public BaseManeuver(String id,
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
  public void acquire() throws Exception {
    LOGGER.info("Attempting maneuver '" + getId() + "'");
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
  public void release() throws Exception {
    LOGGER.info("Completing maneuver '" + getId() + "'");
    // Make sure this process is allowed to complete this maneuver
    if (!getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot complete maneuver '" + getId() + "' that is not being attempted in this process.");
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
  public <T> void set(Resource<T> resource, T state) throws Exception {
    if (!getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot set state of resource '" + resource.getId() + "' from maneuver '" + getId() + "' that is not being attempted in this process.");
    }
    if (!getWrites().contains(resource)) {
      throw new IllegalStateException("Cannot set state of resource '" + resource.getId() + "' from maneuver '" + getId() + "' that does not write it.");
    }
    resource.setState(getId(), state);
  }

  protected abstract ManeuverLock getLock();
}
