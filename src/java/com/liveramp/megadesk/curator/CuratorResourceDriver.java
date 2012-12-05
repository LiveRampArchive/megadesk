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

import com.liveramp.megadesk.condition.ConditionWatcher;
import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.resource.ResourceLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;

public class CuratorResourceDriver implements ResourceDriver {

  private static final String RESOURCES_PATH = "/megadesk/resources";

  private final CuratorFramework curator;
  private final String path;
  private final ResourceLock lock;

  public CuratorResourceDriver(CuratorFramework curator, String id) throws Exception {
    this.curator = curator;
    this.path = ZkPath.append(RESOURCES_PATH, id);
    this.lock = new CuratorResourceLock(curator, path);
  }

  @Override
  public ResourceLock getLock() throws Exception {
    return lock;
  }

  @Override
  public byte[] read(ConditionWatcher watcher) throws Exception {
    if (watcher == null) {
      return curator.getData().forPath(path);
    } else {
      return curator.getData().usingWatcher(new CuratorResourceDataWatcher(watcher)).forPath(path);
    }
  }

  @Override
  public void write(byte[] data) throws Exception {
    curator.setData().forPath(path, data);
  }
}
