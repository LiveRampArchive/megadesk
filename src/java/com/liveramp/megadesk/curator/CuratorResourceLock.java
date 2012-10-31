package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.ResourceReadLock;
import com.liveramp.megadesk.resource.ResourceWriteLock;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.locks.InterProcessMutex;

import java.util.Arrays;
import java.util.List;

public class CuratorResourceLock {

  private static final int SLEEP_TIME_MS = 5000;

  private static final String READ_LOCK_PATH = "r";
  private static final String WRITE_LOCK_PATH = "w";
  private static final String INTERNAL_LOCK_PATH = "lock";

  private final CuratorFramework curator;
  private final InterProcessMutex internalLock;
  private final ResourceReadLock readLock;
  private final ResourceWriteLock writeLock;
  private final String readLockPath;
  private final String writeLockPath;

  public CuratorResourceLock(CuratorFramework curator,
                             String path) {
    this.curator = curator;
    this.internalLock = new InterProcessMutex(curator, ZkPath.append(path, INTERNAL_LOCK_PATH));
    this.readLockPath = ZkPath.append(path, READ_LOCK_PATH);
    this.writeLockPath = ZkPath.append(path, WRITE_LOCK_PATH);
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

  private byte[] getWriteLockOwner() throws Exception {
    return curator.getData().forPath(writeLockPath);
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
    byte[] owner = getWriteLockOwner();
    return owner != null && Arrays.equals(owner, step.getId().getBytes());
  }

  private class CuratorResourceReadLock implements ResourceReadLock {

    @Override
    public void acquire(Step step) throws Exception {
      if (isReadLockOwner(step)) {
        return;
      }
      while (true) {
        internalLock.acquire();
        try {
          if (getWriteLockOwner() == null) {
            doAcquireReadLock(step);
            return;
          }
        } finally {
          internalLock.release();
        }
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(Step step) throws Exception {
      if (!isReadLockOwner(step)) {
        throw new RuntimeException("Cannot release resource read lock " + readLockPath + " in step " + step.getId() + " that did not acquire it.");
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
        internalLock.acquire();
        try {
          if (getWriteLockOwner() == null && getReaders().size() == 0) {
            doAcquireWriteLock(step);
            return;
          }
        } finally {
          internalLock.release();
        }
        Thread.sleep(SLEEP_TIME_MS);
      }
    }

    @Override
    public void release(Step step) throws Exception {
      if (!isWriteLockOwner(step)) {
        throw new RuntimeException("Cannot release resource write lock " + writeLockPath + " in step " + step.getId() + " that did not acquire it.");
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
