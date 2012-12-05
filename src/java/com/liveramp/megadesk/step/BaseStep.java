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

package com.liveramp.megadesk.step;

import com.liveramp.megadesk.Megadesk;
import com.liveramp.megadesk.condition.Condition;
import com.liveramp.megadesk.condition.ConditionWatcher;
import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.driver.MainDriver;
import com.liveramp.megadesk.driver.StepDriver;
import com.liveramp.megadesk.resource.Resource;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;

public abstract class BaseStep implements Step {

  private static final Logger LOGGER = Logger.getLogger(BaseStep.class);

  private final String id;
  private final MainDriver mainDriver;
  private final StepDriver driver;

  public BaseStep(Megadesk megadesk,
                  String id) throws Exception {
    this(megadesk.getMainDriver(), megadesk.getStepDriver(id), id);
  }

  public BaseStep(MainDriver mainDriver,
                  StepDriver driver,
                  String id) {
    this.id = id;
    this.mainDriver = mainDriver;
    this.driver = driver;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public boolean isReady(ConditionWatcher watcher) throws Exception {
    // Check all conditions
    for (Condition condition : conditions()) {
      if (!condition.check(watcher)) {
        return false;
      }
    }
    // Check all dependencies
    for (Dependency dependency : dependencies()) {
      for (Resource resource : dependency.getResources()) {
        if (resource.getWriteLock().isOwnedByAnother(id, watcher)) {
          return false;
        }
        if (!dependency.check(watcher)) {
          return false;
        }
      }
    }
    // Check all writes
    for (Resource write : writes()) {
      if (write.getWriteLock().isOwnedByAnother(id, watcher)
          || write.getReadLock().isOwnedByAnother(id, watcher)) {
        return false;
      }
    }
    return true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public boolean acquire(ConditionWatcher watcher) throws Exception {
    LOGGER.info("Acquiring step '" + getId() + "'");
    driver.getLock().acquire();
    // Acquire resources lock to avoid dead locks
    mainDriver.getResourcesLock().acquire();
    try {
      if (isReady(watcher)) {
        // If ready, acquire all locks in order
        for (Dependency dependency : dependencies()) {
          for (Resource resource : dependency.getResources()) {
            resource.getReadLock().acquire(getId(), true);
          }
        }
        for (Resource write : writes()) {
          write.getWriteLock().acquire(getId(), true);
        }
        LOGGER.info("Acquired step '" + id + "'");
        return true;
      }
    } finally {
      mainDriver.getResourcesLock().release();
    }
    LOGGER.info("Could not acquire step '" + id + "'. Waiting.");
    return false;
  }

  @Override
  public void release() throws Exception {
    LOGGER.info("Completing step '" + getId() + "'");
    // Make sure this process is allowed to complete this step
    if (!driver.getLock().isAcquiredInThisProcess()) {
      throw new IllegalStateException("Cannot complete step '" + getId() + "' that is not acquired by this process.");
    }
    // Release all locks in order
    for (Dependency dependency : dependencies()) {
      for (Resource resource : dependency.getResources()) {
        resource.getReadLock().release(getId());
      }
    }
    for (Resource write : writes()) {
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
    if (!writes().contains(resource)) {
      throw new IllegalStateException("Cannot set data of resource '" + resource.getId() + "' from step '" + getId() + "' that does not write it.");
    }
    resource.write(getId(), data);
  }

  @Override
  public List<Condition> conditions() {
    return Collections.emptyList();
  }

  @Override
  public List<Dependency> dependencies() {
    return Collections.emptyList();
  }

  @Override
  public List<Resource> writes() {
    return Collections.emptyList();
  }
}
