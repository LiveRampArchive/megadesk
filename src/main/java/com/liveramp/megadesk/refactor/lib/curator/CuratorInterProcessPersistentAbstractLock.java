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

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;

import com.liveramp.megadesk.refactor.node.Paths;

public abstract class CuratorInterProcessPersistentAbstractLock implements InterProcessPersistentLock {

  private static final String INTERNAL_LOCK_PATH = "__lock";
  private static final String OWNERS_PATH = "__owners";

  private final String path;
  private final CuratorFramework curator;

  protected final InterProcessMutex volatileLock;

  public CuratorInterProcessPersistentAbstractLock(CuratorFramework curator, String path) throws Exception {
    this(curator, path, new InterProcessMutex(curator, Paths.append(path, INTERNAL_LOCK_PATH)));
  }

  public CuratorInterProcessPersistentAbstractLock(CuratorFramework curator, String path, InterProcessMutex volatileLock) throws Exception {
    this.curator = curator;
    this.path = path;
    // Ensure paths
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    new EnsurePath(getOwnersPath()).ensure(curator.getZookeeperClient());
    this.volatileLock = volatileLock;
  }

  private String getOwnersPath() {
    return Paths.append(path, OWNERS_PATH);
  }

  protected void acquirePersistentLock(String owner, boolean persistent) throws Exception {
    curator.create()
        .withMode(persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL)
        .forPath(Paths.append(getOwnersPath(), owner));
  }

  protected void releasePersistentLock(String owner) throws Exception {
    curator.delete().forPath(Paths.append(getOwnersPath(), owner));
  }

  public List<String> getOwners() throws Exception {
    return curator.getChildren().forPath(getOwnersPath());
  }

  protected boolean isOwnedBy(String owner) throws Exception {
    return getOwners().contains(owner);
  }

  protected boolean isOwned() throws Exception {
    return getOwners().size() != 0;
  }
}
