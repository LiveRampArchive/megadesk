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

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.base.state.Local;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.recipes.gear.Outcome;
import com.liveramp.megadesk.recipes.queue.Batch;

import java.util.List;

public abstract class BatchConsumerOperator<VALUE> extends Operator {

  private final Batch batch;

  public BatchConsumerOperator(Batch batch, BaseDependency<Variable> dependency, Pipeline pipeline) {
    super(BaseDependency.<Variable>builder()
        .reads((List) dependency.reads())
        .writes((List) dependency.writes())
        .writes(batch.getInput(), batch.getOutput())
        .build(),
        pipeline);
    this.batch = batch;
  }

  @Override
  public Outcome check(Context context) {
    Outcome check = super.check(context);
    if (check == Outcome.SUCCESS) {
      ImmutableList currentBatch = (ImmutableList) batch.read(context);
      if (!currentBatch.isEmpty()) {
        return Outcome.SUCCESS;
      } else {
        batch.pop(context);
        return Outcome.STANDBY;
      }
    } else {
      return check;
    }
  }

  @Override
  public Outcome execute(Context context) throws Exception {
    ImmutableList currentBatch = (ImmutableList) batch.read(context);
    Outcome outcome = this.consume(context, currentBatch);
    if (outcome == Outcome.SUCCESS) {
      batch.pop(context);
      return outcome;
    } else {
      return outcome;
    }
  }

  public abstract Outcome consume(Context context, ImmutableList<VALUE> batch);
}
