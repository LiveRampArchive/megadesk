package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;
import com.netflix.curator.utils.EnsurePath;
import org.apache.log4j.Logger;

import java.util.List;

public class CuratorResourceLock {

  private static final Logger LOGGER = Logger.getLogger(CuratorResourceLock.class);

  private static final int SLEEP_TIME_MS = 5000;

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

  private void doAcquireReadLock(String owner) throws Exception {
    curator.create().forPath(ZkPath.append(readersPath, owner));
  }

  private void doAcquireWriteLock(String owner) throws Exception {
    curator.setData().forPath(writerPath, owner.getBytes());
  }

  private String getWriteLockOwner() throws Exception {
    byte[] result = curator.getData().forPath(writerPath);
    return (result == null || result.length == 0) ? null : new String(result);
  }

  private void setWriteLockOwner(Step step) throws Exception {
    curator.setData().forPath(writerPath, step == null ? null : step.getId().getBytes());
  }

  private List<String> getReaders() throws Exception {
    return curator.getChildren().forPath(readersPath);
  }

  private boolean isReadLockOwner(String owner) throws Exception {
    List<String> readers = getReaders();
    return readers.contains(owner);
  }

  private boolean isReadLockOwner(String owner, String state) throws Exception {
    String currentState = resource.getState();
    return state.equals(currentState) && isReadLockOwner(owner);
  }

  private boolean isWriteLockOwner(String owner) throws Exception {
    String currentOwner = getWriteLockOwner();
    return currentOwner != null && currentOwner.equals(owner);
  }

  private class CuratorResourceReadLock implements ResourceReadLock {

    @Override
    public void acquire(String owner, String state, boolean persistent) throws Exception {
      if (isReadLockOwner(owner, state)) {
        return;
      }
      while (true) {
        String writeLockOwner;
        String currentState;
        internalLock.acquire();
        try {
          currentState = resource.getState();
          writeLockOwner = getWriteLockOwner();
          if (writeLockOwner == null && state.equals(currentState)) {
            doAcquireReadLock(owner);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info(owner + " could not acquire resource read lock on '" + resource.getId() +
            "' because there is either already a writer: '" + writeLockOwner + "' or desired state: '" + state +
            "' does not match current state: '" + currentState + "'");
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(String owner) throws Exception {
      if (!isReadLockOwner(owner)) {
        throw new RuntimeException("Cannot release resource read lock on '" + resource.getId() + "' for owner '" + owner + "' that did not acquire it.");
      }
      internalLock.acquire();
      try {
        curator.delete().forPath(ZkPath.append(readersPath, owner));
      } finally {
        internalLock.release();
      }
    }
  }

  private class CuratorResourceWriteLock implements ResourceWriteLock {

    @Override
    public void acquire(String owner, boolean persistent) throws Exception {
      if (isWriteLockOwner(owner)) {
        return;
      }
      while (true) {
        String writeLockOwner;
        List<String> readers;
        internalLock.acquire();
        try {
          writeLockOwner = getWriteLockOwner();
          readers = getReaders();
          if (writeLockOwner == null && readers.size() == 0) {
            doAcquireWriteLock(owner);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info("'" + owner + "' could not acquire resource write lock on '" + resource.getId() + "' because there is either a writer: '" + writeLockOwner + "' or readers: " + getReaders());
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(String owner) throws Exception {
      if (!isWriteLockOwner(owner)) {
        throw new RuntimeException("Cannot release resource write lock on '" + resource.getId() + "' for owner '" + owner + "' that did not acquire it.");
      }
      internalLock.acquire();
      try {
        setWriteLockOwner(null);
      } finally {
        internalLock.release();
      }
    }
  }
}
