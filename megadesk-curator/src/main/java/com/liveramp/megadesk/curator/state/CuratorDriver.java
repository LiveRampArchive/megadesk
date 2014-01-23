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

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.lib.BaseDriver;
import com.liveramp.megadesk.state.lib.filesystem_tools.SerializationHandler;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;

public class CuratorDriver<VALUE> {

  public static <VALUE> Driver<VALUE> build(
      String path,
      CuratorFramework framework,
      SerializationHandler<VALUE> serializer) {

    ReadWriteLock executionLock = new CuratorReadWriteLock(new InterProcessReadWriteLock(framework, path + "/executionLock"));
    ReadWriteLock persistenceLock = new CuratorReadWriteLock(new InterProcessReadWriteLock(framework, path + "/persistenceLock"));
    Persistence<VALUE> persistence = new CuratorPersistence<VALUE>(framework, path, serializer);
    Reference<VALUE> reference = new CuratorReference<VALUE>(path);

    return new BaseDriver<VALUE>(reference, persistence, persistenceLock, executionLock);
  }
}
