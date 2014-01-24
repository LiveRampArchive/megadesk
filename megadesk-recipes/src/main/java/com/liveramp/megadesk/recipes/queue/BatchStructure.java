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
package com.liveramp.megadesk.recipes.queue;

import com.google.common.collect.ImmutableList;
import com.liveramp.megadesk.core.transaction.Executor;
import com.liveramp.megadesk.recipes.pipeline.DriverFactory;

public class BatchStructure<VALUE> {

  private final Batch<VALUE> batch;
  private Executor executor;

  public BatchStructure(Batch<VALUE> batch, Executor executor) {
    this.batch = batch;
    this.executor = executor;
  }

  public static <VALUE> BatchStructure<VALUE> getByName(String name, DriverFactory factory, Executor executor) {
    return new BatchStructure<VALUE>(
        new Batch<VALUE>(factory.<ImmutableList>get(name + "-input", ImmutableList.of()),
            factory.<ImmutableList>get(name + "-output", ImmutableList.of()),
            factory.<Boolean>get(name + "-frozen", false)), executor);
  }

  public void append(VALUE value) {
    try {
      executor.execute(batch.getAppendTransaction(value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public ImmutableList<VALUE> readBatch() {
    try {
      executor.execute(batch.getTransferTransaction());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return batch.getOutput().persistence().read();
  }

  public void popBatch() {
    try {
      executor.execute(batch.getEraseTransaction());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
