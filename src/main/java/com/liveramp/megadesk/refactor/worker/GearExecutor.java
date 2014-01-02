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

package com.liveramp.megadesk.refactor.worker;

import com.liveramp.megadesk.refactor.attempt.Outcome;
import com.liveramp.megadesk.refactor.gear.Gear;
import com.liveramp.megadesk.refactor.gear.Gears;
import com.liveramp.megadesk.refactor.register.Participant;
import com.liveramp.megadesk.refactor.register.Registers;

public class GearExecutor {

  private boolean register(Gear gear, Participant participant) throws Exception {
    return Registers.register(Gears.getHierarchyRegisters(gear), participant) &&
               Registers.register(Gears.getReadRegisters(gear), participant) &&
               Registers.register(Gears.getWriteRegisters(gear), participant);
  }

  private void unregister(Gear gear, Participant participant) throws Exception {
    Registers.unregister(Gears.getHierarchyRegisters(gear), participant);
    Registers.unregister(Gears.getReadRegisters(gear), participant);
    Registers.unregister(Gears.getWriteRegisters(gear), participant);
  }

  public Outcome execute(Gear gear) throws Exception {

    Participant participant = new Participant(gear.getNode().getPath().get());

    // Acquire master lock
    gear.getNode().getMasterLock().acquire();

    // Determine if gear should run
    boolean shouldRun = false;
    try {
      shouldRun = register(gear, participant) && gear.isRunnable();
      if (!shouldRun) {
        unregister(gear, participant);
      }
    } finally {
      // Release master lock
      gear.getNode().getMasterLock().release();
    }

    // Run gear
    if (shouldRun) {
      try {
        return gear.run();
      } catch (Throwable t) {
        // TODO
        return Outcome.FAILURE;
      } finally {
        // Unregister
        unregister(gear, participant);
      }
    } else {
      return Outcome.WAIT;
    }
  }
}
