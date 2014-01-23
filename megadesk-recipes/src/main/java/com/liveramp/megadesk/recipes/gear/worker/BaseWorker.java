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

package com.liveramp.megadesk.recipes.gear.worker;

import java.util.Arrays;
import java.util.List;

import com.liveramp.megadesk.recipes.gear.Gear;

public abstract class BaseWorker implements Worker {

  @Override
  public void run(Gear... gears) {
    for (Gear gear : gears) {
      run(gear);
    }
  }

  @Override
  public void run(List<Gear> gears) {
    for (Gear gear : gears) {
      run(gear);
    }
  }

  @Override
  public void complete(Gear gear) throws InterruptedException {
    complete(Arrays.asList(gear));
  }

  @Override
  public void complete(Gear... gears) throws InterruptedException {
    complete(Arrays.asList(gears));
  }

  @Override
  public void complete(List<Gear> gears) throws InterruptedException {
    run(gears);
    stop();
    join();
  }
}
