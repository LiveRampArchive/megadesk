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

import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import com.liveramp.megadesk.refactor.node.Paths;
import com.liveramp.megadesk.refactor.register.Participant;
import com.liveramp.megadesk.refactor.register.Register;

public class CuratorRegister implements Register {

  private final CuratorFramework curator;
  private final String path;

  public CuratorRegister(CuratorFramework curator, String path) throws Exception {
    this.curator = curator;
    this.path = path;
    // Ensure paths
    // new EnsurePath(path).ensure(curator.getZookeeperClient());
  }

  private String getPath() {
    return path;
  }

  @Override
  public boolean register(Participant participant) throws Exception {
    register(participant, true);
    return true; // TODO
  }

  private void register(Participant participant, boolean persistent) throws Exception {
    curator.create()
        .withMode(persistent ? CreateMode.PERSISTENT : CreateMode.EPHEMERAL)
        .forPath(Paths.append(getPath(), participant.getId()));
  }

  @Override
  public void unregister(Participant participant) throws Exception {
    curator.delete().forPath(Paths.append(getPath(), participant.getId()));
  }

  @Override
  public List<Participant> participants() throws Exception {
    List<Participant> result = new ArrayList<Participant>();
    for (String path : curator.getChildren().forPath(getPath())) {
      result.add(new Participant(path));
    }
    return result;
  }
}
