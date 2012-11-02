package com.liveramp.megadesk.maneuver;

import com.liveramp.megadesk.device.Device;
import com.liveramp.megadesk.device.Read;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class BaseManeuver implements Maneuver {

  private static final Logger LOGGER = Logger.getLogger(BaseManeuver.class);

  private String id;
  private List<Read> reads = Collections.emptyList();
  private List<Device> writes = Collections.emptyList();

  public BaseManeuver(String id) {
    this.id = id;
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
  public List<Device> getWrites() {
    return writes;
  }

  @Override
  public Maneuver reads(Read... reads) {
    this.reads = Arrays.asList(reads);
    return this;
  }

  @Override
  public Maneuver writes(Device... writes) {
    this.writes = Arrays.asList(writes);
    return this;
  }

  @Override
  public void acquire() throws Exception {
    LOGGER.info("Attempting maneuver '" + getId() + "'");
    // Acquire all locks in order
    // TODO: potential dead locks
    getLock().acquire();
    for (Read read : reads) {
      read.getDevice().getReadLock().acquire(getId(), read.getStatusCheck(), true);
    }
    for (Device write : writes) {
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
      read.getDevice().getReadLock().release(getId());
    }
    for (Device write : writes) {
      write.getWriteLock().release(getId());
    }
    getLock().release();
  }

  @Override
  public <T> void write(Device<T> device, T status) throws Exception {
    if (!getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot set status of device '" + device.getId() + "' from maneuver '" + getId() + "' that is not being attempted in this process.");
    }
    if (!getWrites().contains(device)) {
      throw new IllegalStateException("Cannot set status of device '" + device.getId() + "' from maneuver '" + getId() + "' that does not write it.");
    }
    device.setStatus(getId(), status);
  }

  protected abstract ManeuverLock getLock();
}
