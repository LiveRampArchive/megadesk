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

  private static final String READ_LOCK_PATH = "r";
  private static final String WRITE_LOCK_PATH = "w";
  private static final String INTERNAL_LOCK_PATH = "lock";

  private final CuratorFramework curator;
  private final CuratorResource resource;
  private final InterProcessMutex internalLock;
  private final ResourceReadLock readLock;
  private final ResourceWriteLock writeLock;
  private final String readLockPath;
  private final String writeLockPath;

  public CuratorResourceLock(CuratorFramework curator,
                             CuratorResource resource) throws Exception {
    this.curator = curator;
    this.resource = resource;
    String internalLockPath = ZkPath.append(resource.getPath(), INTERNAL_LOCK_PATH);
    new EnsurePath(internalLockPath).ensure(curator.getZookeeperClient());
    this.internalLock = new InterProcessMutex(curator, internalLockPath);
    this.readLockPath = ZkPath.append(resource.getPath(), READ_LOCK_PATH);
    this.writeLockPath = ZkPath.append(resource.getPath(), WRITE_LOCK_PATH);

    new EnsurePath(readLockPath).ensure(curator.getZookeeperClient());
    new EnsurePath(writeLockPath).ensure(curator.getZookeeperClient());
    this.readLock = new CuratorResourceReadLock();
    this.writeLock = new CuratorResourceWriteLock();
  }

  public ResourceReadLock getReadLock() {
    return readLock;
  }

  public ResourceWriteLock getWriteLock() {
    return writeLock;
  }

  private void doAcquireReadLock(Step step) throws Exception {
    curator.create().forPath(ZkPath.append(readLockPath, step.getId()));
  }

  private void doAcquireWriteLock(Step step) throws Exception {
    curator.setData().forPath(writeLockPath, step.getId().getBytes());
  }

  private String getWriteLockOwner() throws Exception {
    byte[] result = curator.getData().forPath(writeLockPath);
    return (result == null || result.length == 0) ? null : new String(result);
  }

  private void setWriteLockOwner(Step step) throws Exception {
    curator.setData().forPath(writeLockPath, step == null ? null : step.getId().getBytes());
  }

  private List<String> getReaders() throws Exception {
    return curator.getChildren().forPath(readLockPath);
  }

  private boolean isReadLockOwner(Step step) throws Exception {
    List<String> readers = getReaders();
    return readers.contains(step.getId());
  }

  private boolean isWriteLockOwner(Step step) throws Exception {
    String owner = getWriteLockOwner();
    return owner != null && owner.equals(step.getId());
  }

  private class CuratorResourceReadLock implements ResourceReadLock {

    @Override
    public void acquire(Step step) throws Exception {
      if (isReadLockOwner(step)) {
        return;
      }
      while (true) {
        String writeLockOwner;
        internalLock.acquire();
        try {
          writeLockOwner = getWriteLockOwner();
          if (writeLockOwner == null) {
            doAcquireReadLock(step);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info("Step " + step.getId() + " could not acquire resource read lock on " + resource.getId() + " because there is already a writer: " + writeLockOwner);
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(Step step) throws Exception {
      if (!isReadLockOwner(step)) {
        throw new RuntimeException("Cannot release resource read lock on " + resource.getId() + " in step " + step.getId() + " that did not acquire it.");
      }
      internalLock.acquire();
      try {
        curator.delete().forPath(ZkPath.append(readLockPath, step.getId()));
      } finally {
        internalLock.release();
      }
    }
  }

  private class CuratorResourceWriteLock implements ResourceWriteLock {

    @Override
    public void acquire(Step step) throws Exception {
      if (isWriteLockOwner(step)) {
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
            doAcquireWriteLock(step);
            return;
          }
        } finally {
          internalLock.release();
        }
        LOGGER.info("Step " + step.getId() + " could not acquire resource write lock on " + resource.getId() + " because there is either a writer: " + writeLockOwner + " or readers: " + getReaders());
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(Step step) throws Exception {
      if (!isWriteLockOwner(step)) {
        throw new RuntimeException("Cannot release resource write lock on " + resource.getId() + " in step " + step.getId() + " that did not acquire it.");
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
