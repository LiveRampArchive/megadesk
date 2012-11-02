package com.liveramp.megadesk.step;

import com.liveramp.megadesk.driver.StepDriver;
import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.serialization.Serialization;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class BaseStep<T, SELF extends BaseStep> implements Step<T, SELF> {

  private static final Logger LOGGER = Logger.getLogger(BaseStep.class);

  private final String id;
  private final StepDriver driver;
  private final Serialization<T> dataSerialization;
  private List<Read> reads = Collections.emptyList();
  private List<Resource> writes = Collections.emptyList();

  public BaseStep(String id, StepDriver driver, Serialization<T> dataSerialization) {
    this.id = id;
    this.driver = driver;
    this.dataSerialization = dataSerialization;
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
  @SuppressWarnings("unchecked")
  public SELF reads(Read... reads) {
    this.reads = Arrays.asList(reads);
    return (SELF) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public SELF writes(Resource... writes) {
    this.writes = Arrays.asList(writes);
    return (SELF) this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void acquire() throws Exception {
    LOGGER.info("Attempting step '" + getId() + "'");
    // Acquire all locks in order
    // TODO: potential dead locks
    driver.getLock().acquire();
    for (Read read : reads) {
      read.getResource().getReadLock().acquire(getId(), true);
    }
    for (Resource write : writes) {
      write.getWriteLock().acquire(getId(), true);
    }
  }

  @Override
  public void release() throws Exception {
    LOGGER.info("Completing step '" + getId() + "'");
    // Make sure this process is allowed to complete this step
    if (!driver.getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot complete step '" + getId() + "' that is not acquired by this process.");
    }
    // Release all locks in order
    for (Read read : reads) {
      read.getResource().getReadLock().release(getId());
    }
    for (Resource write : writes) {
      write.getWriteLock().release(getId());
    }
    driver.getLock().release();
  }

  @Override
  @SuppressWarnings("unchecked")
  public void write(Resource resource, Object data) throws Exception {
    if (!driver.getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot set data of resource '" + resource.getId() + "' from step '" + getId() + "' that is not acquired by this process.");
    }
    if (!getWrites().contains(resource)) {
      throw new IllegalStateException("Cannot set data of resource '" + resource.getId() + "' from step '" + getId() + "' that does not write it.");
    }
    resource.setData(getId(), data);
  }

  @Override
  public T getData() throws Exception {
    if (!driver.getLock().isAcquiredInThisProcess()) {
      driver.getLock().acquire();
      try {
        return dataSerialization.deserialize(driver.getData());
      } finally {
        driver.getLock().release();
      }
    } else {
      return dataSerialization.deserialize(driver.getData());
    }
  }

  @Override
  public void setData(T data) throws Exception {
    if (!driver.getLock().isAcquiredInThisProcess()) {
      driver.getLock().acquire();
      try {
        driver.setData(dataSerialization.serialize(data));
      } finally {
        driver.getLock().release();
      }
    } else {
      driver.setData(dataSerialization.serialize(data));
    }
  }
}
