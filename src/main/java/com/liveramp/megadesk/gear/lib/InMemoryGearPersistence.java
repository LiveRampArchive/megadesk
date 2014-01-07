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

package com.liveramp.megadesk.gear.lib;

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.attempt.Attempt;
import com.liveramp.megadesk.gear.GearPersistence;
import com.liveramp.megadesk.gear.State;

public class InMemoryGearPersistence implements GearPersistence {

  private State state;
  private final List<Attempt> attempts;

  public InMemoryGearPersistence() {
    attempts = Lists.newArrayList();
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void setState(State state) {
    this.state = state;
  }

  @Override
  public List<Attempt> getAttempts() {
    return attempts;
  }

  @Override
  public Attempt createAttempt() {
    Attempt result = null; // TODO
    attempts.add(result);
    return result;
  }
}
