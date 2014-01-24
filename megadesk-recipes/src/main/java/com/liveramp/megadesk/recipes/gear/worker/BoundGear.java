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

import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.recipes.gear.Gear;

public class BoundGear {

  private final Gear gear;
  private final Binding binding;

  public BoundGear(Gear gear, Binding binding) {
    this.gear = gear;
    this.binding = binding;
  }

  public Gear gear() {
    return gear;
  }

  public Binding binding() {
    return binding;
  }
}
