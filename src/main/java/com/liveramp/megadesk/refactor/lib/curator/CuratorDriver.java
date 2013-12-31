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

import com.netflix.curator.RetryPolicy;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.imps.CuratorFrameworkState;
import com.netflix.curator.retry.RetryNTimes;

import com.liveramp.megadesk.refactor.driver.Driver;
import com.liveramp.megadesk.refactor.lock.Lock;
import com.liveramp.megadesk.refactor.node.Node;
import com.liveramp.megadesk.refactor.node.Path;

public class CuratorDriver implements Driver {

  private final CuratorFramework curator;
  private final Lock syncLock;

  public CuratorDriver(String connectString) {
    this(connectString, 1000, new RetryNTimes(10, 500));
  }

  public CuratorDriver(String connectString,
                       int connectionTimeoutMs,
                       RetryPolicy retryPolicy) {
    this(CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(connectionTimeoutMs)
        .retryPolicy(retryPolicy)
        .connectString(connectString)
        .build());
  }

  public CuratorDriver(CuratorFramework curator) {
    if (curator.getState() != CuratorFrameworkState.STARTED) {
      curator.start();
    }
    this.curator = curator;
    this.syncLock = new CuratorLock("/_sync_lock");
  }

  @Override
  public Node getNode(Path path) {
    return new CuratorNode(path, syncLock);
  }

  @Override
  public List<Path> getChildren(Path path) {
    return null;  // TODO
  }

  @Override
  public Path getParent(Path path) {
    return null;  // TODO
  }
}
