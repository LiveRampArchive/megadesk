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

package com.liveramp.megadesk.base.state;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import com.liveramp.megadesk.core.state.Lock;

public class MultiLock implements Lock {

  private final Collection<Lock> locks;
  private final Set<Lock> acquiredLocks;

  public MultiLock(Collection<Lock> locks) {
    this.locks = ImmutableList.copyOf(locks);
    this.acquiredLocks = Sets.newHashSet();
  }

  @Override
  public void lock() {
    for (Lock lock : locks) {
      lockAndRemember(lock, acquiredLocks);
    }
  }

  @Override
  public boolean tryLock() {
    for (Lock lock : locks) {
      if (!tryLockAndRemember(lock, acquiredLocks)) {
        unlockAndRemember(acquiredLocks);
        return false;
      }
    }
    return true;
  }

  @Override
  public void unlock() {
    unlockAndRemember(acquiredLocks);
  }

  private static boolean tryLockAndRemember(Lock lock, Set<Lock> acquiredLocks) {
    boolean result = lock.tryLock();
    if (result) {
      acquiredLocks.add(lock);
    }
    return result;
  }

  private static void lockAndRemember(Lock lock, Set<Lock> acquiredLocks) {
    lock.lock();
    acquiredLocks.add(lock);
  }

  private static void unlockAndRemember(Set<Lock> acquiredLocks) {
    Iterator<Lock> lockIterator = acquiredLocks.iterator();
    while (lockIterator.hasNext()) {
      lockIterator.next().unlock();
      lockIterator.remove();
    }
  }
}
