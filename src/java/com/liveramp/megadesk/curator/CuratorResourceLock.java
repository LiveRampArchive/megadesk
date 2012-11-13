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

package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.DependencyWatcher;
import com.liveramp.megadesk.resource.ResourceLock;
import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.utils.EnsurePath;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.util.List;

public class CuratorResourceLock implements ResourceLock {

  private static final long SLEEP_TIME_MS = 1000;
  private static final Logger LOGGER = Logger.getLogger(CuratorResourceLock.class);

  private static final String READERS_PATH = "__readers";
  private static final String WRITER_PATH = "__writer";
  private static final String INTERNAL_LOCK_PATH = "__lock";

  private final CuratorFramework curator;
  private final String path;
  private final InterProcessMutex internalLock;
  private final ResourceReadLock readLock;
  private final ResourceWriteLock writeLock;
  private final String readersPath;
  private final String writerPath;

  public CuratorResourceLock(CuratorFramework curator,
                             String path) throws Exception {
    this.curator = curator;
    this.path = path;
    String internalLockPath = ZkPath.append(path, INTERNAL_LOCK_PATH);
    new EnsurePath(internalLockPath).ensure(curator.getZookeeperClient());
    this.internalLock = new InterProcessMutex(curator, internalLockPath);
    this.readersPath = ZkPath.append(path, READERS_PATH);
    this.writerPath = ZkPath.append(path, WRITER_PATH);
    new EnsurePath(readersPath).ensure(curator.getZookeeperClient());
    new EnsurePath(writerPath).ensure(curator.getZookeeperClient());
    this.readLock = new CuratorResourceReadLock();
    this.writeLock = new CuratorResourceWriteLock();
  }

  public ResourceReadLock getReadLock() {
    return readLock;
  }

  public ResourceWriteLock getWriteLock() {
    return writeLock;
  }

  private void doAcquireReadLock(String owner, boolean persistent) throws Exception {
    curator.create()
        .withMode(persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL)
        .forPath(ZkPath.append(readersPath, owner));
  }

  private void doAcquireWriteLock(String owner, boolean persistent) throws Exception {
    curator.create()
        .withMode(persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL)
        .forPath(ZkPath.append(writerPath, owner));
  }

  private String getWriteLockOwner() throws Exception {
    return getWriteLockOwner(null);
  }

  private String getWriteLockOwner(DependencyWatcher watcher) throws Exception {
    List<String> writers;
    if (watcher == null) {
      writers = curator.getChildren().forPath(writerPath);
    } else {
      writers = curator.getChildren().usingWatcher(new CuratorResourceLockWatcher(watcher)).forPath(writerPath);
    }
    if (writers.size() != 1) {
      return null;
    } else {
      return writers.get(0);
    }
  }

  private List<String> getReadLockOwners(DependencyWatcher watcher) throws Exception {
    if (watcher == null) {
      return curator.getChildren().forPath(readersPath);
    } else {
      return curator.getChildren().usingWatcher(new CuratorResourceLockWatcher(watcher)).forPath(readersPath);
    }
  }

  private boolean isReadLockOwner(String owner) throws Exception {
    List<String> readers = getReadLockOwners(null);
    return readers.contains(owner);
  }

  private boolean isWriteLockOwner(String owner) throws Exception {
    String currentOwner = getWriteLockOwner();
    return currentOwner != null && currentOwner.equals(owner);
  }

  private boolean isReadLockOwnedByAnother(String id, DependencyWatcher watcher) throws Exception {
    String owner = getWriteLockOwner(watcher);
    return owner != null && !owner.equals(id);
  }

  private boolean isWriteLockOwnedByAnother(String id, DependencyWatcher watcher) throws Exception {
    List<String> owners = getReadLockOwners(watcher);
    // Is owned by another only if there are readers
    return owners.size() != 0 // no readers
        && (owners.size() > 1 // many readers
        || (owners.size() == 1 && !owners.contains(id))); // one reader, but not the given one
  }

  private class CuratorResourceReadLock implements ResourceReadLock {

    @Override
    public void acquire(String owner, boolean persistent) throws Exception {
      LOGGER.info("'" + owner + "' acquiring read lock on resource '" + path + "'");
      while (true) {
        internalLock.acquire();
        try {
          if (isReadLockOwner(owner)) {
            // Already owned
            return;
          }
          if (!isWriteLockOwnedByAnother(owner, null)) {
            doAcquireReadLock(owner, persistent);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info(owner + " could not acquire resource read lock on '" + path
            + "' because there is already another writer: '" + getWriteLockOwner() + "'");
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(String owner) throws Exception {
      LOGGER.info("'" + owner + "' releasing read lock on resource '" + path + "'");
      internalLock.acquire();
      try {
        if (!isReadLockOwner(owner)) {
          throw new IllegalStateException("Cannot release resource read lock on '" + path
              + "' by owner '" + owner + "' that did not acquire it.");
        }
        curator.delete().forPath(ZkPath.append(readersPath, owner));
      } finally {
        internalLock.release();
      }
    }

    @Override
    public List<String> getOwners() throws Exception {
      return getReadLockOwners(null);
    }

    @Override
    public boolean isOwnedByAnother(String id) throws Exception {
      return isReadLockOwnedByAnother(id, null);
    }

    @Override
    public boolean isOwnedByAnother(String id, DependencyWatcher watcher) throws Exception {
      return isReadLockOwnedByAnother(id, watcher);
    }
  }

  private class CuratorResourceWriteLock implements ResourceWriteLock {

    @Override
    public void acquire(String owner, boolean persistent) throws Exception {
      LOGGER.info("'" + owner + "' acquiring write lock on resource '" + path + "'");
      while (true) {
        internalLock.acquire();
        try {
          if (isWriteLockOwner(owner)) {
            // Already owned
            return;
          }
          if (!isWriteLockOwnedByAnother(owner, null) && !isReadLockOwnedByAnother(owner, null)) {
            doAcquireWriteLock(owner, persistent);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info("'" + owner + "' could not acquire resource write lock on '" + path
            + "' because there is either a writer: '" + getWriteLockOwner()
            + "' or another reader: " + getReadLockOwners(null));
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(String owner) throws Exception {
      LOGGER.info("'" + owner + "' releasing write lock on resource '" + path + "'");
      internalLock.acquire();
      try {
        if (!isWriteLockOwner(owner)) {
          throw new IllegalStateException("Cannot release resource write lock on '" + path
              + "' by owner '" + owner + "' that did not acquire it.");
        }
        curator.delete().forPath(ZkPath.append(writerPath, owner));
      } finally {
        internalLock.release();
      }
    }

    @Override
    public String getOwner() throws Exception {
      return getWriteLockOwner();
    }

    @Override
    public boolean isOwnedByAnother(String id) throws Exception {
      return isWriteLockOwnedByAnother(id, null);
    }

    @Override
    public boolean isOwnedByAnother(String id, DependencyWatcher watcher) throws Exception {
      return isWriteLockOwnedByAnother(id, watcher);
    }
  }
}
