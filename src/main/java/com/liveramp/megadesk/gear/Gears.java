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

package com.liveramp.megadesk.gear;

import java.util.ArrayList;
import java.util.List;

import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.register.Register;

public final class Gears {

  private Gears() {
  }

  public static <T extends Gear> List<Node> getNodes(List<T> gears) {
    List<Node> result = new ArrayList<Node>();
    for (Gear gear : gears) {
      result.add(gear.getNode());
    }
    return result;
  }

  public static List<Register> getHierarchyRegisters(Gear gear) {
    List<Register> result = new ArrayList<Register>();
    // Write on itself
    result.add(gear.getNode().getWriteRegister());
    // Read on hierarchy
    Node parent = gear.getNode().getParent();
    while (parent != null) {
      result.add(parent.getReadRegister());
      parent = parent.getParent();
    }
    return result;
  }
}
