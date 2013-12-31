/**
 *  Copyright 2014 LiveRamp
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

package com.liveramp.megadesk.refactor.lib.curator;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

public class CuratorInterProcessPersistentLock
    extends CuratorInterProcessPersistentAbstractLock
    implements InterProcessPersistentLock {

  private static final long SLEEP_TIME_MS = 100;

  public CuratorInterProcessPersistentLock(
      CuratorFramework curator,
      String path,
      InterProcessMutex volatileLock) throws Exception {
    super(curator, path, volatileLock);
  }

  @Override
  public void acquire(String owner) throws Exception {
    while (true) {
      if (acquire(owner, 1, TimeUnit.NANOSECONDS)) {
        return;
      } else {
        Thread.sleep(SLEEP_TIME_MS);
      }
    }
  }

  @Override
  public boolean acquire(String owner, long time, TimeUnit unit) throws Exception {
    if (volatileLock.acquire(time, unit)) {
      if (!isOwned()) {
        // Acquire persistent lock and hang on to volatile lock
        acquirePersistentLock(owner, true);
        return true;
      } else if (!isOwnedBy(owner)) {
        // Persistent lock is owned by another owner, release volatile lock
        volatileLock.release();
        return false;
      } else {
        // Otherwise the lock is already owned by the given owner, reenter
        return true;
      }
    } else {
      return false;
    }
  }

  @Override
  public void release(String owner) throws Exception {
    // Persistent lock may only be released if volatile lock is owned and persistent lock is owned by the given owner,
    // hence attempt to quickly reenter only if needed.
    if (volatileLock.isAcquiredInThisProcess() &&
        isOwnedBy(owner) &&
        volatileLock.acquire(1, TimeUnit.NANOSECONDS)) {
      try {
        releasePersistentLock(owner);
      } finally {
        volatileLock.release();
      }
    }
  }
}
