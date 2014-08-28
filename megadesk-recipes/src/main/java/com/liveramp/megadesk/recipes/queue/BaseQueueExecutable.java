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

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.core.transaction.TransactionExecutor;

public abstract class BaseQueueExecutable<VALUE, OUTPUT> implements MegadeskPersistentQueue<VALUE, OUTPUT> {

  private final BaseQueue<VALUE, OUTPUT> queue;
  private TransactionExecutor executor;

  public BaseQueueExecutable(BaseQueue<VALUE, OUTPUT> queue, TransactionExecutor executor) {
    this.queue = queue;
    this.executor = executor;
  }

  public void append(VALUE... values) {
    append(Lists.newArrayList(values));
  }

  public void append(List<VALUE> values) {
    try {
      executor.execute(queue.getAppendTransaction(values));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public OUTPUT read() {
    try {
      return queue.internalRead(executor.execute(queue.getTransferTransaction()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void pop() {
    try {
      executor.execute(queue.getPopTransaction());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public BaseQueue<VALUE, OUTPUT> getQueue() {
    return queue;
  }

  public UnsafeQueue<VALUE> getUnsafeQueue() {
    return queue.getUnsafeQueue(executor);
  }
}
