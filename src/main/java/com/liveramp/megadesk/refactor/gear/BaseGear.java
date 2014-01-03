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

package com.liveramp.megadesk.refactor.gear;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.liveramp.megadesk.refactor.node.Node;
import com.liveramp.megadesk.refactor.persistence.Persistence;

public abstract class BaseGear implements Gear {

  private List<Node> reads;
  private List<Node> writes;

  public BaseGear() {
    this.reads = Collections.emptyList();
    this.writes = Collections.emptyList();
  }

  @Override
  public Persistence getPersistence() {
    return null;
  }

  @Override
  public List<Node> reads() {
    return reads;
  }

  @Override
  public List<Node> writes() {
    return writes;
  }

  public BaseGear reads(Node... nodes) {
    this.reads = Arrays.asList(nodes);
    return this;
  }

  public BaseGear writes(Node... nodes) {
    this.writes = Arrays.asList(nodes);
    return this;
  }

  @Override
  public String toString() {
    return "[" + BaseGear.class.getSimpleName() + " " + getNode().getPath().get() + "]";
  }
}
