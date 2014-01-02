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

import java.util.List;

import org.apache.curator.framework.CuratorFramework;

import com.liveramp.megadesk.refactor.lock.Lock;
import com.liveramp.megadesk.refactor.node.BaseNode;
import com.liveramp.megadesk.refactor.node.Node;
import com.liveramp.megadesk.refactor.node.Path;
import com.liveramp.megadesk.refactor.node.Paths;
import com.liveramp.megadesk.refactor.register.Participant;
import com.liveramp.megadesk.refactor.register.Register;
import com.liveramp.megadesk.refactor.register.Registers;

public class CuratorNode extends BaseNode implements Node {

  private static final String READ_REGISTER_PATH = "__read";
  private static final String WRITE_REGISTER_PATH = "__write";

  private final Register readRegister;
  private final Register writeRegister;
  private final Node parent;

  public CuratorNode(CuratorDriver driver, String path) throws Exception {
    this(driver.getCuratorFramework(), new Path(path), driver.getMasterLock());
  }

  public CuratorNode(CuratorFramework curator, Path path, Lock masterLock) throws Exception {
    super(path, masterLock);
    this.readRegister = new CuratorReadRegister(curator, Paths.append(path.get(), READ_REGISTER_PATH));
    this.writeRegister = new CuratorWriteRegister(curator, Paths.append(path.get(), WRITE_REGISTER_PATH));
    this.parent = Paths.sanitize(path.get()).equals("/") ? null : new CuratorNode(curator, Paths.parent(path), masterLock);
  }

  private class CuratorReadRegister extends CuratorBaseRegister implements Register {

    public CuratorReadRegister(CuratorFramework curator, String path) throws Exception {
      super(curator, path);
    }

    @Override
    public boolean register(Participant participant) throws Exception {
      if (Registers.isEmptyOrRegistered(CuratorNode.this.getWriteRegister(), participant)) {
        if (!Registers.isRegistered(this, participant)) {
          register(participant, true);
        }
        return true;
      } else {
        return false;
      }
    }
  }

  private class CuratorWriteRegister extends CuratorBaseRegister implements Register {

    public CuratorWriteRegister(CuratorFramework curator, String path) throws Exception {
      super(curator, path);
    }

    @Override
    public boolean register(Participant participant) throws Exception {
      if (Registers.isEmptyOrRegistered(CuratorNode.this.getReadRegister(), participant)) {
        List<Participant> participants = participants();
        // Register is empty, register the given participant and report success
        if (participants.isEmpty()) {
          register(participant, true);
          return true;
        } else {
          // Register is already registered, return success iff it's registered by the given participant
          return participants.contains(participant);
        }
      } else {
        return false;
      }
    }
  }

  @Override
  public Register getReadRegister() {
    return readRegister;
  }

  @Override
  public Register getWriteRegister() {
    return writeRegister;
  }

  @Override
  public Node getParent() {
    return parent;
  }
}
