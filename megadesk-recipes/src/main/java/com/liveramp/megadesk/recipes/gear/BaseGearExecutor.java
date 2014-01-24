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

package com.liveramp.megadesk.recipes.gear;

import org.apache.log4j.Logger;

import com.liveramp.megadesk.base.transaction.BaseExecutor;
import com.liveramp.megadesk.base.transaction.ExecutionResult;
import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.core.transaction.Executor;

public class BaseGearExecutor implements GearExecutor {

  private static final Logger LOG = Logger.getLogger(BaseGearExecutor.class);

  private final Executor executor = new BaseExecutor();

  @Override
  public Outcome execute(Gear gear) {
    return execute(gear, null);
  }

  @Override
  public Outcome execute(Gear gear, Binding binding) {
    try {
      ExecutionResult<Outcome> executionResult = executor.tryExecute(gear, binding);
      if (executionResult.executed()) {
        return executionResult.result();
      } else {
        return Outcome.STANDBY;
      }
    } catch (Exception e) {
      LOG.error(e);
      throw new RuntimeException(e); // TODO
      // return Outcome.FAILURE;
    }
  }
}
