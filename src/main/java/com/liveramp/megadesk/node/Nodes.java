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

package com.liveramp.megadesk.node;

import java.util.ArrayList;
import java.util.List;

import com.liveramp.megadesk.register.Register;

public final class Nodes {

  private Nodes() {
  }

  public static List<Register> getReadRegisters(List<Node> nodes) {
    List<Register> result = new ArrayList<Register>();
    for (Node node : nodes) {
      result.add(node.getReadRegister());
    }
    return result;
  }

  public static List<Register> getWriteRegisters(List<Node> nodes) {
    List<Register> result = new ArrayList<Register>();
    for (Node node : nodes) {
      result.add(node.getWriteRegister());
    }
    return result;
  }

  public static List<Register> getHierarchyRegisters(Node node) {
    List<Register> result = new ArrayList<Register>();
    // Write on itself
    result.add(node.getWriteRegister());
    // Read on hierarchy
    Node parent = node.getParent();
    while (parent != null) {
      result.add(parent.getReadRegister());
      parent = parent.getParent();
    }
    return result;
  }
}
