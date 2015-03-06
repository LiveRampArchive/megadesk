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

import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.recipes.iteration.Iteration;

public class BaseGearIteration implements Iteration {

  private static final Logger LOG = LoggerFactory.getLogger(BaseGearIteration.class);

  private final TransactionExecutor executor = new BaseTransactionExecutor();
  private final Gear gear;
  private final Binding binding;

  public BaseGearIteration(Gear gear) {
    this(gear, null);
  }

  public BaseGearIteration(Gear gear, Binding binding) {
    this.gear = gear;
    this.binding = binding;
  }

  @Override
  public Iteration call() throws Exception {
    Outcome outcome = executor.execute(gear, binding);
    switch (outcome) {
      case SUCCESS:
        return this;
      case FAILURE:
        return null;
      case STANDBY:
        return this;
      case ABANDON:
        return null;
      default:
        throw new IllegalStateException(); // TODO
    }
  }
}
