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

import com.liveramp.megadesk.old.gear.BaseOldGear;
import com.liveramp.megadesk.old.gear.OldGear;
import com.liveramp.megadesk.old.node.Node;
import com.liveramp.megadesk.old.node.Path;

public abstract class CuratorOldGear extends BaseOldGear implements OldGear {

  private Node node;

  public CuratorOldGear(CuratorDriver driver,
                        String path) throws Exception {
    this(driver, new Path(path));
  }

  public CuratorOldGear(CuratorDriver driver,
                        Path path) throws Exception {
    super(driver.getMasterLock());
    this.node = new CuratorNode(driver.getCuratorFramework(), path);
  }

  @Override
  public Node getNode() {
    return node;
  }
}
