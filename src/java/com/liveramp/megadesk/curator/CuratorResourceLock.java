package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.state.StateCheck;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.utils.EnsurePath;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.util.List;

public class CuratorResourceLock {

  private static final Logger LOGGER = Logger.getLogger(CuratorResourceLock.class);

  private static final int SLEEP_TIME_MS = 1000;

  private static final String READERS_PATH = "readers";
  private static final String WRITER_PATH = "writer";
  private static final String INTERNAL_LOCK_PATH = "lock";

  private final CuratorFramework curator;
  private final CuratorResource resource;
  private final InterProcessMutex internalLock;
  private final ResourceReadLock readLock;
  private final ResourceWriteLock writeLock;
  private final String readersPath;
  private final String writerPath;

  public CuratorResourceLock(CuratorFramework curator,
                             CuratorResource resource) throws Exception {
    this.curator = curator;
    this.resource = resource;
    String internalLockPath = ZkPath.append(resource.getPath(), INTERNAL_LOCK_PATH);
    new EnsurePath(internalLockPath).ensure(curator.getZookeeperClient());
    this.internalLock = new InterProcessMutex(curator, internalLockPath);
    this.readersPath = ZkPath.append(resource.getPath(), READERS_PATH);
    this.writerPath = ZkPath.append(resource.getPath(), WRITER_PATH);

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
    List<String> writers = curator.getChildren().forPath(writerPath);
    if (writers.size() != 1) {
      return null;
    } else {
      return writers.get(0);
    }
  }

  private List<String> getReadLockOwners() throws Exception {
    return curator.getChildren().forPath(readersPath);
  }

  private boolean isReadLockOwner(String owner) throws Exception {
    List<String> readers = getReadLockOwners();
    return readers.contains(owner);
  }

  private boolean isWriteLockOwner(String owner) throws Exception {
    String currentOwner = getWriteLockOwner();
    return currentOwner != null && currentOwner.equals(owner);
  }

  private class CuratorResourceReadLock implements ResourceReadLock {

    @Override
    public void acquire(String owner, StateCheck stateCheck, boolean persistent) throws Exception {
      LOGGER.info("'" + owner + "' acquiring read lock on resource '" + resource.getId() + "' with state check: " + stateCheck);
      while (true) {
        String writeLockOwner;
        internalLock.acquire();
        try {
          if (isReadLockOwner(owner)) {
            // Already owned
            return;
          }
          writeLockOwner = getWriteLockOwner();
          if (writeLockOwner == null && stateCheck.check(resource)) {
            doAcquireReadLock(owner, persistent);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info(owner + " could not acquire resource read lock on '" + resource.getId() +
            "' because there is either already a writer: '" + writeLockOwner + "' or state check: '" + stateCheck +
            "' failed. Resource: " + resource);
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(String owner) throws Exception {
      LOGGER.info("'" + owner + "' releasing read lock on resource '" + resource.getId() + "'");
      internalLock.acquire();
      try {
        if (!isReadLockOwner(owner)) {
          throw new IllegalStateException("Cannot release resource read lock on '" + resource.getId() + "' by owner '" + owner + "' that did not acquire it.");
        }
        curator.delete().forPath(ZkPath.append(readersPath, owner));
      } finally {
        internalLock.release();
      }
    }

    @Override
    public List<String> getOwners() throws Exception {
      return getReadLockOwners();
    }
  }

  private class CuratorResourceWriteLock implements ResourceWriteLock {

    @Override
    public void acquire(String owner, boolean persistent) throws Exception {
      LOGGER.info("'" + owner + "' acquiring write lock on resource '" + resource.getId() + "'");
      while (true) {
        String writeLockOwner;
        List<String> readers;
        internalLock.acquire();
        try {
          writeLockOwner = getWriteLockOwner();
          if (writeLockOwner != null && writeLockOwner.equals(owner)) {
            // Already owned
            return;
          }
          readers = getReadLockOwners();
          if (writeLockOwner == null && readers.size() == 0) {
            doAcquireWriteLock(owner, persistent);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info("'" + owner + "' could not acquire resource write lock on '" + resource.getId() + "' because there is either a writer: '" + writeLockOwner + "' or readers: " + getReadLockOwners());
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(String owner) throws Exception {
      LOGGER.info("'" + owner + "' releasing write lock on resource '" + resource.getId() + "'");
      internalLock.acquire();
      try {
        if (!isWriteLockOwner(owner)) {
          throw new IllegalStateException("Cannot release resource write lock on '" + resource.getId() + "' by owner '" + owner + "' that did not acquire it.");
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
  }
}
