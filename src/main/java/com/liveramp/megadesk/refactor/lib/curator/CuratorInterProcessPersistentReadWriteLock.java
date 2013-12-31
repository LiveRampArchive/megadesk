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

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessReadWriteLock;

import com.liveramp.megadesk.refactor.node.Paths;

public class CuratorInterProcessPersistentReadWriteLock implements InterProcessPersistentReadWriteLock {

  private static final String VOLATILE_LOCK_PATH = "__lock";
  private static final String READ_LOCK_PATH = "__read";
  private static final String WRITE_LOCK_PATH = "__write";

  private final InterProcessReadWriteLock volatileReadWriteLock;
  private final CuratorInterProcessPersistentAbstractLock readLock;
  private final CuratorInterProcessPersistentAbstractLock writeLock;

  public CuratorInterProcessPersistentReadWriteLock(CuratorFramework curator, String path) throws Exception {
    String volatileLockPath = Paths.append(path, VOLATILE_LOCK_PATH);
    // new EnsurePath(volatileLockPath).ensure(curator.getZookeeperClient());
    this.volatileReadWriteLock = new InterProcessReadWriteLock(curator, volatileLockPath);
    readLock = new CuratorInterProcessPersistentLock(curator, Paths.append(path, READ_LOCK_PATH), volatileReadWriteLock.readLock());
    writeLock = new CuratorInterProcessPersistentLock(curator, Paths.append(path, WRITE_LOCK_PATH), volatileReadWriteLock.writeLock());
  }

  @Override
  public InterProcessPersistentLock getReadLock() {
    return readLock;
  }

  @Override
  public InterProcessPersistentLock getWriteLock() {
    return writeLock;
  }
}
