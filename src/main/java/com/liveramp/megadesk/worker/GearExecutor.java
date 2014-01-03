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

package com.liveramp.megadesk.worker;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Gears;
import com.liveramp.megadesk.register.Participant;
import com.liveramp.megadesk.register.Register;
import com.liveramp.megadesk.register.Registers;

public class GearExecutor {

  private static final Logger LOG = Logger.getLogger(GearExecutor.class);

  private boolean register(Gear gear, Participant participant) throws Exception {
    List<Register> registers = new ArrayList<Register>();
    registers.addAll(Gears.getHierarchyRegisters(gear));
    registers.addAll(Gears.getReadRegisters(gear));
    registers.addAll(Gears.getWriteRegisters(gear));
    LOG.info("Attempting to register: " + participant + " in " + registers);
    return Registers.register(registers, participant);
  }

  private void unregister(Gear gear, Participant participant) throws Exception {
    Registers.unregister(Gears.getHierarchyRegisters(gear), participant);
    Registers.unregister(Gears.getReadRegisters(gear), participant);
    Registers.unregister(Gears.getWriteRegisters(gear), participant);
  }

  private boolean shouldRun(Gear gear, Participant participant) throws Exception {
    // Acquire master lock
    LOG.info("Acquiring master lock");
    gear.getNode().getMasterLock().acquire();
    // Determine if gear should run
    boolean shouldRun = false;
    try {
      // Determine if it can be registered
      boolean isRegistered = register(gear, participant);
      LOG.info("Attempting to register " + participant + " for " + gear + ": " + isRegistered);
      // Determine if it is runnable
      boolean isRunnable = false;
      if (isRegistered) {
        isRunnable = gear.isRunnable();
        LOG.info("Determining if " + gear + " is runnable: " + isRunnable);
      }
      // Determine if it should run
      shouldRun = isRegistered && isRunnable;
      // Unregister if it should not run
      if (!shouldRun) {
        LOG.info("Unregistering " + participant + " for " + gear);
        unregister(gear, participant);
      }
    } finally {
      // Release master lock
      LOG.info("Releasing master lock");
      gear.getNode().getMasterLock().release();
    }
    return shouldRun;
  }

  private Outcome run(Gear gear, Participant participant) throws Exception {
    LOG.info("Running " + gear);
    try {
      Outcome outcome = gear.run();
      LOG.info("Ran " + gear + ", outcome: " + outcome);
      return outcome;
    } catch (Throwable t) {
      LOG.info("Gear " + gear + " failed");
      return Outcome.FAILURE;
    } finally {
      // TODO: is locking necessary here since we are only unregistering?
      // Acquire master lock
      LOG.info("Acquiring master lock");
      gear.getNode().getMasterLock().acquire();
      try {
        LOG.info("Unregistering " + participant + " for " + gear);
        unregister(gear, participant);
      } finally {
        // Release master lock
        LOG.info("Releasing master lock");
        gear.getNode().getMasterLock().release();
      }
    }
  }

  public Outcome execute(Gear gear) throws Exception {
    Participant participant = new Participant(gear.getNode().getPath().get());
    // Run gear
    if (shouldRun(gear, participant)) {
      return run(gear, participant);
    } else {
      LOG.info("Gear " + gear + " is waiting");
      return Outcome.WAIT;
    }
  }
}
