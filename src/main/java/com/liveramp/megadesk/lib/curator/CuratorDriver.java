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

package com.liveramp.megadesk.lib.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;

public class CuratorDriver {

  private final CuratorFramework curatorFramework;
  private final CuratorLock masterLock;

  public CuratorDriver(String connectString) throws Exception {
    this(connectString, 1000, new RetryNTimes(10, 500));
  }

  public CuratorDriver(String connectString,
                       int connectionTimeoutMs,
                       RetryPolicy retryPolicy) throws Exception {
    this(CuratorFrameworkFactory.builder()
             .connectionTimeoutMs(connectionTimeoutMs)
             .retryPolicy(retryPolicy)
             .connectString(connectString)
             .build());
  }

  public CuratorDriver(CuratorFramework curatorFramework) throws Exception {
    if (curatorFramework.getState() != CuratorFrameworkState.STARTED) {
      curatorFramework.start();
    }
    this.curatorFramework = curatorFramework;
    this.masterLock = new CuratorLock(curatorFramework, "/__masterlock");
  }

  public CuratorFramework getCuratorFramework() {
    return curatorFramework;
  }

  public CuratorLock getMasterLock() {
    return masterLock;
  }
}
