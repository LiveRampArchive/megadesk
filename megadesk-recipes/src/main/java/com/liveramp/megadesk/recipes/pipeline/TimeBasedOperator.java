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

package com.liveramp.megadesk.recipes.pipeline;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.gear.Gear;
import com.liveramp.megadesk.recipes.gear.Outcome;

import java.util.List;

public abstract class TimeBasedOperator extends Operator implements Gear {

  protected TimeBasedOperator(List<Variable<? extends TimestampedValue>> reads, List<Variable<? extends TimestampedValue>> writes, Pipeline pipeline) {
    super(BaseDependency.<Variable>builder().reads((List) reads).writes((List) writes).build(), pipeline);
  }

  @Override
  public Outcome check(Context context) {
    Outcome check = super.check(context);
    if (check == Outcome.SUCCESS) {
      long oldestRead = Long.MAX_VALUE;
      for (Variable<TimestampedValue> driver : this.dependency().reads()) {
        oldestRead = Math.min(context.read(driver.reference()).timestamp(), oldestRead);
      }
      long youngestWrite = 0;
      for (Variable<TimestampedValue> driver : this.dependency().writes()) {
        youngestWrite = Math.max(context.read(driver.reference()).timestamp(), youngestWrite);
      }
      if (oldestRead >= youngestWrite) {
        return Outcome.SUCCESS;
      } else {
        return Outcome.STANDBY;
      }
    } else {
      return check;
    }
  }
}
