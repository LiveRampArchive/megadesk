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

package com.liveramp.megadesk.dependency;

import java.util.ArrayList;
import java.util.List;

import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.node.Nodes;
import com.liveramp.megadesk.register.Participant;
import com.liveramp.megadesk.register.Register;
import com.liveramp.megadesk.register.Registers;

public abstract class ReadWriteDependency implements Dependency {

  private final List<Register> registers;

  protected ReadWriteDependency(List<Node> reads,
                                List<Node> writes) {
    this.registers = new ArrayList<Register>();
    this.registers.addAll(Nodes.getReadRegisters(reads));
    this.registers.addAll(Nodes.getWriteRegisters(writes));
  }

  @Override
  public DependencyCheck acquire(Participant participant) throws Exception {
    if (Registers.register(registers, participant) && check()) {
      return DependencyCheck.ACQUIRED;
    } else {
      Registers.unregister(registers, participant);
      return DependencyCheck.STANDBY;
    }
  }

  @Override
  public void release(Participant participant) throws Exception {
    Registers.unregister(registers, participant);
  }

  public abstract boolean check();
}
