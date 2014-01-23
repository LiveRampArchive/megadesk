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

package com.liveramp.megadesk.curator.state;

import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializationHandler;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializedPersistence;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.NodeCache;

public class CuratorPersistence<VALUE> extends SerializedPersistence<VALUE> implements Persistence<VALUE> {

  private final NodeCache cache;
  private CuratorFramework curator;
  private final String path;

  public CuratorPersistence(CuratorFramework curator, String path, SerializationHandler<VALUE> serializer) {
    super(serializer);
    this.curator = curator;
    this.path = path;
    this.cache = new NodeCache(curator, path);

    try {
      if (curator.checkExists().forPath(path) == null) {
        curator.create().forPath(path);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void writeBytes(byte[] serializedObject) {
    try {
      curator.setData().forPath(path, serializedObject);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected byte[] readBytes() {
    return cache.getCurrentData().getData();
  }
}
