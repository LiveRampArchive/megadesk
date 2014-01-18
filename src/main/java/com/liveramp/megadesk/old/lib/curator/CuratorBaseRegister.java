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

package com.liveramp.megadesk.old.lib.curator;

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.EnsurePath;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import com.liveramp.megadesk.old.node.Paths;
import com.liveramp.megadesk.old.register.Participant;
import com.liveramp.megadesk.old.register.Register;
import com.liveramp.megadesk.old.register.Registers;
import com.liveramp.megadesk.utils.FormatUtils;

public abstract class CuratorBaseRegister implements Register {

  private static final String PATH_SEPARATOR = "/";
  private static final String ENTRY_SEPARATOR = "\\|";

  private final CuratorFramework curator;
  private final String path;
  private final PathChildrenCache pathChildrenCache;

  public CuratorBaseRegister(CuratorFramework curator, String path) throws Exception {
    this.curator = curator;
    this.path = path;
    // Ensure paths
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.pathChildrenCache = new PathChildrenCache(curator, path, false);
    this.pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
  }

  @Override
  public void unregister(Participant participant) throws Exception {
    if (Registers.isRegistered(this, participant)) {
      curator.delete().forPath(Paths.append(path, toEntry(participant)));
    }
  }

  @Override
  public List<Participant> participants() throws Exception {
    // TODO: is there a chance that the cache does not reflect the latest updates to this register?
    List<Participant> result = new ArrayList<Participant>();
    for (ChildData child : pathChildrenCache.getCurrentData()) {
      result.add(fromEntry(Paths.filename(child.getPath())));
    }
    return result;
  }

  protected void register(Participant participant, boolean persistent) throws Exception {
    try {
      curator.create()
          .withMode(persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL)
          .forPath(Paths.append(path, toEntry(participant)));
    } catch (KeeperException.NodeExistsException e) {
      // Already registered, do nothing
    }
  }

  // Child nodes may not contain '/' or they will become subdirectories
  protected String toEntry(Participant participant) {
    if (participant.getId().contains(ENTRY_SEPARATOR)) {
      throw new IllegalArgumentException(CuratorBaseRegister.class.getSimpleName() + " entry may not contain " + ENTRY_SEPARATOR);
    }
    return participant.getId().replaceAll(PATH_SEPARATOR, ENTRY_SEPARATOR);
  }

  private Participant fromEntry(String entry) {
    return new Participant(entry.replaceAll(ENTRY_SEPARATOR, PATH_SEPARATOR));
  }

  @Override
  public String toString() {
    try {
      return FormatUtils.formatToString(this, path + ", " + participants());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
