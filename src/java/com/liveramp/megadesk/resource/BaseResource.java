/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.megadesk.resource;

import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.serialization.Serialization;
import org.apache.log4j.Logger;

import java.util.UUID;

public abstract class BaseResource<T> implements Resource<T> {

  private static final Logger LOGGER = Logger.getLogger(BaseResource.class);

  private final String id;
  private final ResourceDriver driver;
  private final Serialization<T> dataSerialization;

  protected BaseResource(String id,
                         ResourceDriver driver,
                         Serialization<T> dataSerialization) {
    this.id = id;
    this.driver = driver;
    this.dataSerialization = dataSerialization;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public T read() throws Exception {
    return read(null);
  }

  @Override
  public T read(DependencyWatcher watcher) throws Exception {
    return dataSerialization.deserialize(driver.read(watcher));
  }

  private void doSetData(T data) throws Exception {
    LOGGER.info("Setting resource '" + id + "' data to: " + data);
    driver.write(dataSerialization.serialize(data));
  }

  @Override
  public void write(T data) throws Exception {
    String owner = "__manual_set_status_" + UUID.randomUUID().toString();
    getWriteLock().acquire(owner, false);
    try {
      doSetData(data);
    } finally {
      getWriteLock().release(owner);
    }
  }

  @Override
  public void write(String owner, T data) throws Exception {
    getWriteLock().acquire(owner, false);
    doSetData(data);
    // Note: do not release the write lock as this assumes the lock is held and will be released later
  }

  @Override
  public ResourceReadLock getReadLock() throws Exception {
    return driver.getLock().getReadLock();
  }

  @Override
  public ResourceWriteLock getWriteLock() throws Exception {
    return driver.getLock().getWriteLock();
  }

  @Override
  public String toString() {
    try {
      return "[BaseResource '" + getId()
          + "', writer: '" + getWriteLock().getOwner()
          + "', readers: " + getReadLock().getOwners() + "]";
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
