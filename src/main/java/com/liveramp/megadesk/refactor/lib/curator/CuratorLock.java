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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.EnsurePath;

import com.liveramp.megadesk.refactor.lock.Lock;

public class CuratorLock implements Lock {

  private final InterProcessMutex lock;

  public CuratorLock(CuratorFramework curator, String path) throws Exception {
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new InterProcessMutex(curator, path);
  }

  @Override
  public void acquire() throws Exception {
    lock.acquire();
  }

  @Override
  public void release() throws Exception {
    lock.release();
  }
}
