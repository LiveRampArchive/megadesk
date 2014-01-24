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

import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.recipes.gear.Gear;

public abstract class BaseWorker implements Worker {

  @Override
  public void run(Gear gear) {
    run(Arrays.asList(gear), null);
  }

  @Override
  public void run(Gear... gears) {
    run(Arrays.asList(gears), null);
  }

  @Override
  public void run(List<Gear> gears) {
    run(gears, null);
  }

  @Override
  public void run(Binding binding, Gear... gears) {
    run(Arrays.asList(gears), binding);
  }

  @Override
  public void run(List<Gear> gears, Binding binding) {
    for (Gear gear : gears) {
      run(gear, binding);
    }
  }

  @Override
  public void complete(Gear gear) throws InterruptedException {
    complete(Arrays.asList(gear), null);
  }

  @Override
  public void complete(Gear... gears) throws InterruptedException {
    complete(Arrays.asList(gears), null);
  }

  @Override
  public void complete(List<Gear> gears) throws InterruptedException {
    complete(gears, null);
  }

  @Override
  public void complete(Gear gear, Binding binding) throws InterruptedException {
    complete(Arrays.asList(gear), binding);
  }

  @Override
  public void complete(Binding binding, Gear... gears) throws InterruptedException {
    complete(Arrays.asList(gears), binding);
  }

  @Override
  public void complete(List<Gear> gears, Binding binding) throws InterruptedException {
    run(gears, binding);
    stop();
    join();
  }
}
