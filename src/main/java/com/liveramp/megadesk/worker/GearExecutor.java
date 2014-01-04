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

import org.apache.log4j.Logger;

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.dependency.DependencyCheck;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.register.Participant;

public class GearExecutor {

  private static final Logger LOG = Logger.getLogger(GearExecutor.class);

  public Outcome execute(Gear gear) throws Exception {
    Participant participant = new Participant(gear.getNode().getPath().get());

    DependencyCheck dependency = acquireDependency(gear, participant);

    Outcome outcome;

    if (dependency == DependencyCheck.ACQUIRED) {
      LOG.info("Running " + gear);
      outcome = run(gear);
      releaseDependency(gear, participant);
    } else if (dependency == DependencyCheck.ABANDON) {
      return Outcome.ABANDON;
    } else if (dependency == DependencyCheck.STANDBY) {
      return Outcome.STANDBY;
    } else {
      throw new RuntimeException("Unknown: " + dependency);
    }

    LOG.info("Executed " + gear + ", outcome: " + outcome);

    return outcome;
  }

  private DependencyCheck acquireDependency(Gear gear, Participant participant) throws Exception {
    gear.getMasterLock().acquire();
    try {
      return gear.getDependency().acquire(participant);
    } finally {
      gear.getMasterLock().release();
    }
  }

  private void releaseDependency(Gear gear, Participant participant) throws Exception {
    gear.getMasterLock().acquire();
    try {
      gear.getDependency().release(participant);
    } finally {
      gear.getMasterLock().release();
    }
  }

  private Outcome run(Gear gear) throws Exception {
    try {
      return gear.run();
    } catch (Throwable t) {
      return Outcome.FAILURE;
    }
  }
}
