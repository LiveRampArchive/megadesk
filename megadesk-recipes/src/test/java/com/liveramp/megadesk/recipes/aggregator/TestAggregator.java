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

import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;

public class TestAggregator extends BaseTestCase {

  private final static class SumAggregator implements Aggregator<Integer> {
    @Override
    public Integer initialValue() {
      return 0;
    }

    @Override
    public Integer aggregate(Integer integer, Integer newValue) {
      return integer + newValue;
    }
  }

  @Test
  public void testAggregators() throws Exception {
    Variable<Integer> variable = new InMemoryLocal<Integer>();
    Aggregator<Integer> sumAggregator = new SumAggregator();

    InterProcessAggregator<Integer> aggregator1 = new InterProcessAggregator<Integer>(variable, sumAggregator);
    InterProcessAggregator<Integer> aggregator2 = new InterProcessAggregator<Integer>(variable, sumAggregator);
    InterProcessAggregator<Integer> aggregator3 = new InterProcessAggregator<Integer>(variable, sumAggregator);

    iterAggregate(10, aggregator1);
    iterAggregate(5, aggregator2);
    iterAggregate(3, aggregator3);

    Assert.assertEquals(Integer.valueOf(36), aggregator1.readRemote());
  }

  private void iterAggregate(int number, InterProcessAggregator<Integer> aggregator) throws Exception {
    for (int i = 0; i < number; ++i) {
      aggregator.aggregateLocal(1);
    }
    aggregator.aggregateRemote();
    for (int i = 0; i < number; ++i) {
      aggregator.aggregateLocal(1);
    }
    aggregator.aggregateRemote();
  }

  @Test
  public void testKeyedAggregator() throws Exception {
    Variable<ImmutableMap<String, Integer>> variable = new InMemoryLocal<ImmutableMap<String, Integer>>();
    Aggregator<Integer> aggregator = new SumAggregator();

    InterProcessKeyedAggregator<String, Integer> aggregator1 = new InterProcessKeyedAggregator<String, Integer>(variable, aggregator);
    InterProcessKeyedAggregator<String, Integer> aggregator2 = new InterProcessKeyedAggregator<String, Integer>(variable, aggregator);
    InterProcessKeyedAggregator<String, Integer> aggregator3 = new InterProcessKeyedAggregator<String, Integer>(variable, aggregator);

    aggregator1.aggregateLocal("a", 1);
    aggregator1.aggregateLocal("b", 1);
    aggregator1.aggregateLocal("c", 1);

    aggregator2.aggregateLocal("a", 2);
    aggregator2.aggregateLocal("d", 2);

    aggregator3.aggregateLocal("c", 3);
    aggregator3.aggregateLocal("d", 3);
    aggregator3.aggregateLocal("e", 3);

    aggregator1.aggregateRemote();
    aggregator2.aggregateRemote();
    aggregator3.aggregateRemote();

    assertEquals(Integer.valueOf(3), aggregator1.readRemote("a"));
    assertEquals(Integer.valueOf(1), aggregator1.readRemote("b"));
    assertEquals(Integer.valueOf(4), aggregator1.readRemote("c"));
    assertEquals(Integer.valueOf(5), aggregator1.readRemote("d"));
    assertEquals(Integer.valueOf(3), aggregator1.readRemote("e"));
  }
}
