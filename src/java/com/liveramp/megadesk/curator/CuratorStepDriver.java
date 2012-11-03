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

import com.liveramp.megadesk.driver.StepDriver;
import com.liveramp.megadesk.step.StepLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

public class CuratorStepDriver implements StepDriver {

  private static final String STEPS_PATH = "/megadesk/steps";

  private final CuratorFramework curator;
  private final String path;
  private final CuratorStepLock lock;

  public CuratorStepDriver(CuratorFramework curator, String id) throws Exception {
    this.curator = curator;
    this.path = ZkPath.append(STEPS_PATH, id);
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorStepLock(curator, path);
  }

  @Override
  public StepLock getLock() {
    return lock;
  }

  @Override
  public byte[] getData() throws Exception {
    return curator.getData().forPath(path);
  }

  @Override
  public void setData(byte[] data) throws Exception {
    curator.setData().forPath(path, data);
  }
}
