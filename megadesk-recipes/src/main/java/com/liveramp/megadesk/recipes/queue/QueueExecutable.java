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

import com.liveramp.megadesk.base.state.BaseVariable;
import com.liveramp.megadesk.base.state.Name;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.recipes.state.DriverFactory;

public class QueueExecutable<VALUE> extends BaseQueueExecutable<VALUE, VALUE> implements MegadeskPersistentQueue<VALUE, VALUE> {

  public QueueExecutable(Queue<VALUE> queue, TransactionExecutor executor) {
    super(queue, executor);
  }

  public static <VALUE> QueueExecutable<VALUE> getQueueByName(String name,
                                                              DriverFactory<ImmutableList<VALUE>> listFactory,
                                                              DriverFactory<Boolean> boolFactory) {
    return new QueueExecutable<VALUE>(
        new Queue<VALUE>(
            new BaseVariable<ImmutableList<VALUE>>(new Name<ImmutableList<VALUE>>(name + "input"), listFactory.get(name + "-input", ImmutableList.<VALUE>of())),
            new BaseVariable<ImmutableList<VALUE>>(new Name<ImmutableList<VALUE>>(name + "output"), listFactory.get(name + "-output", ImmutableList.<VALUE>of())),
            new BaseVariable<Boolean>(new Name<Boolean>(name + "frozen"), (boolFactory.get(name + "-frozen", false)))),
        new BaseTransactionExecutor()
    );
  }

  @Override
  public Queue<VALUE> getQueue() {
    return (Queue<VALUE>)super.getQueue();
  }
}
