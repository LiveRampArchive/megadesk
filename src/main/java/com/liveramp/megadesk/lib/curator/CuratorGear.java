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

import com.liveramp.megadesk.gear.BaseGear;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.node.Path;

public abstract class CuratorGear extends BaseGear implements Gear {

  private Node node;

  public CuratorGear(CuratorDriver driver,
                     String path) throws Exception {
    this(driver, new Path(path));
  }

  public CuratorGear(CuratorDriver driver,
                     Path path) throws Exception {
    this.node = new CuratorNode(driver.getCuratorFramework(), path, driver.getMasterLock());
  }

  @Override
  public Node getNode() {
    return node;
  }
}
