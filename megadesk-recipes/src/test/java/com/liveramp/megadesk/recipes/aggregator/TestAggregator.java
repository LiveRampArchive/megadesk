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

package com.liveramp.megadesk.recipes.aggregator;

import junit.framework.Assert;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.recipes.transaction.Read;
import com.liveramp.megadesk.test.BaseTestCase;

public class TestAggregator extends BaseTestCase {

  @Test
  public void testAggregators() throws Exception {
    final Variable<Integer> variable = new InMemoryLocal<Integer>();

    Aggregator<Integer> sumAggregator = new Aggregator<Integer>() {
      @Override
      public Integer initialValue() {
        return 0;
      }

      @Override
      public Integer aggregate(Integer integer, Integer newValue) {
        return integer + newValue;
      }
    };

    InterProcessAggregator<Integer> aggregator =
        new InterProcessAggregator<Integer>(variable, sumAggregator);

    aggregator.resetRemote();

    Thread thread1 = makeThread(10, variable, sumAggregator);
    Thread thread2 = makeThread(3, variable, sumAggregator);
    Thread thread3 = makeThread(5, variable, sumAggregator);

    thread1.start();
    thread2.start();
    thread3.start();

    thread1.join();
    thread2.join();
    thread3.join();

    Assert.assertEquals(Integer.valueOf(36), new BaseTransactionExecutor().execute(new Read<Integer>(variable)));
  }

  private Thread makeThread(
      final int amount,
      final Variable<Integer> variable,
      final Aggregator<Integer> sumAggregator) {

    final InterProcessAggregator<Integer> aggregator =
        new InterProcessAggregator<Integer>(variable, sumAggregator);

    Runnable runnable = new Runnable() {

      @Override
      public void run() {
        for (int i = 0; i < amount; i++) {
          aggregator.aggregateLocal(1);
        }
        aggregator.aggregateRemote();
        for (int i = 0; i < amount; i++) {
          aggregator.aggregateLocal(1);
        }
        aggregator.aggregateRemote();
      }
    };

    return new Thread(runnable);
  }
}
