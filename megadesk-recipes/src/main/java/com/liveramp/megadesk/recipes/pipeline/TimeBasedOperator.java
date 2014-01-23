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

import java.util.List;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.gear.Gear;
import com.liveramp.megadesk.recipes.gear.Outcome;

public abstract class TimeBasedOperator extends Operator implements Gear {

  protected TimeBasedOperator(List<TimestampedDriver> reads, List<TimestampedDriver> writes, Pipeline pipeline) {
    super(BaseDependency.<Driver>builder().reads((List)reads).writes((List)writes).build(), pipeline);
  }

  @Override
  public Outcome check(Context context) {
    Outcome check = super.check(context);
    if (check == Outcome.SUCCESS) {
      long oldestRead = Long.MAX_VALUE;
      for (Driver driver : this.dependency().reads()) {
        if (driver instanceof TimestampedDriver) {
          oldestRead = Math.min(((TimestampedDriver)driver).modified(), oldestRead);
        }
      }
      long youngestWrite = 0;
      for (Driver driver : this.dependency().writes()) {
        if (driver instanceof TimestampedDriver) {
          youngestWrite = Math.max(((TimestampedDriver)driver).modified(), youngestWrite);
        }
      }
      if (oldestRead > youngestWrite) {
        return Outcome.SUCCESS;
      } else {
        return Outcome.STANDBY;
      }
    } else {
      return check;
    }
  }
}
