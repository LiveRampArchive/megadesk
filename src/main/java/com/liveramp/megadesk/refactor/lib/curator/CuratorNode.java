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

import com.liveramp.megadesk.refactor.lock.Lock;
import com.liveramp.megadesk.refactor.node.BaseNode;
import com.liveramp.megadesk.refactor.node.Node;
import com.liveramp.megadesk.refactor.node.Path;
import com.liveramp.megadesk.refactor.node.Paths;

public class CuratorNode extends BaseNode implements Node {

  private static final String READ_REGISTER_PATH = "__read";
  private static final String WRITE_REGISTER_PATH = "__write";

  public CuratorNode(CuratorDriver driver, String path) throws Exception {
    this(driver.getCuratorFramework(), new Path(path), driver.getMasterLock());
  }

  public CuratorNode(CuratorFramework curator, Path path, Lock masterLock) throws Exception {
    super(path,
             masterLock,
             new CuratorRegister(curator, Paths.append(path.get(), READ_REGISTER_PATH)),
             new CuratorRegister(curator, Paths.append(path.get(), WRITE_REGISTER_PATH)),
             Paths.sanitize(path.get()).equals("/") ? null : new CuratorNode(curator, Paths.parent(path), masterLock));
  }
}
