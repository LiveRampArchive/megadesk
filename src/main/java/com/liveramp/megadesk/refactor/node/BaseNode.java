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

package com.liveramp.megadesk.refactor.node;

import com.liveramp.megadesk.refactor.lock.Lock;
import com.liveramp.megadesk.refactor.register.Register;

public abstract class BaseNode implements Node {

  private final Path path;
  private final Lock masterLock;
  private final Register readRegister;
  private final Register writeRegister;
  private final Node parent;

  public BaseNode(Path path,
                  Lock masterLock,
                  Register readRegister,
                  Register writeRegister,
                  Node parent) {
    this.path = path;
    this.masterLock = masterLock;
    this.readRegister = readRegister;
    this.writeRegister = writeRegister;
    this.parent = parent;
  }

  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public Lock getMasterLock() {
    return masterLock;
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
