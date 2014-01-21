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

import com.liveramp.megadesk.transaction.BaseExecutor;
import com.liveramp.megadesk.transaction.ExecutionResult;
import com.liveramp.megadesk.transaction.Executor;

public class BaseGearExecutor implements GearExecutor {

  private final Executor executor = new BaseExecutor();

  public Outcome execute(Gear gear) {
    try {
      return executor.execute(gear);
    } catch (Exception e) {
      return Outcome.FAILURE;
    }
  }

  @Override
  public Outcome tryExecute(Gear gear) {
    try {
      ExecutionResult<Outcome> result = executor.tryExecute(gear);
      if (result.executed()) {
        return result.result();
      } else {
        return Outcome.STANDBY;
      }
    } catch (Exception e) {
      return Outcome.FAILURE;
    }
  }
}
