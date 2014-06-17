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

package com.liveramp.megadesk.curator.state;

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.recipes.locks.InterProcessLock;

import com.liveramp.megadesk.core.state.Lock;

public class CuratorLock implements Lock {

  private final InterProcessLock lock;

  public CuratorLock(InterProcessLock lock) {
    this.lock = lock;
  }

  @Override
  public void lock() {
    try {
      lock.acquire();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean tryLock() {
    try {
      return lock.acquire(0, TimeUnit.NANOSECONDS);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unlock() {
    try {
      lock.release();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isLockOwned() {
    return lock.isAcquiredInThisProcess();
  }
}
