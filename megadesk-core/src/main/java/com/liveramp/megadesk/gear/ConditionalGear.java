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

package com.liveramp.megadesk.gear;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.Context;

public abstract class ConditionalGear extends BaseGear implements Gear {

  public ConditionalGear() {
  }

  public ConditionalGear(BaseDependency<Driver> dependency) {
    super(dependency);
  }

  public abstract Outcome check(Context context);

  public abstract Outcome execute(Context context) throws Exception;

  @Override
  public final Outcome run(Context context) throws Exception {
    Outcome check = check(context);
    if (check == Outcome.SUCCESS) {
      return execute(context);
    } else {
      return check;
    }
  }
}
