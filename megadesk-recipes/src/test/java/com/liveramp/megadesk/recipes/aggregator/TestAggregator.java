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

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.test.BaseTestCase;

import static org.junit.Assert.assertEquals;

public class TestAggregator extends BaseTestCase {

  private final static class SumAggregator implements Aggregator<Integer, Integer> {
    @Override
    public Integer initialValue() {
      return 0;
    }

    @Override
    public Integer aggregate(Integer aggregate, Integer value) {
      return aggregate + value;
    }

    @Override
    public Integer merge(Integer lhs, Integer rhs) {
      return lhs + rhs;
    }
  }

  private class SetAggregator<T> implements Aggregator<T, ImmutableSet<T>> {

    @Override
    public ImmutableSet<T> initialValue() {
      return ImmutableSet.of();
    }

    @Override
    public ImmutableSet<T> aggregate(T value, ImmutableSet<T> aggregate) {
      Set<T> result = Sets.newHashSet();
      result.addAll(aggregate);
      result.add(value);
      return ImmutableSet.copyOf(result);
    }

    @Override
    public ImmutableSet<T> merge(ImmutableSet<T> lhs, ImmutableSet<T> rhs) {
      Set<T> result = Sets.newHashSet(lhs);
      result.addAll(rhs);
      return ImmutableSet.copyOf(result);
    }
  }

  @Test
  public void testAggregators() throws Exception {
    Variable<Integer> variable = new InMemoryLocal<Integer>();
    Aggregator<Integer, Integer> sumAggregator = new SumAggregator();

    InterProcessAggregator<Integer, Integer> aggregator1 = new InterProcessAggregator<Integer, Integer>(variable, sumAggregator);
    InterProcessAggregator<Integer, Integer> aggregator2 = new InterProcessAggregator<Integer, Integer>(variable, sumAggregator);
    InterProcessAggregator<Integer, Integer> aggregator3 = new InterProcessAggregator<Integer, Integer>(variable, sumAggregator);

    iterAggregate(10, aggregator1);
    iterAggregate(5, aggregator2);
    iterAggregate(3, aggregator3);

    Assert.assertEquals(Integer.valueOf(36), aggregator1.read());
  }

  private void iterAggregate(int number, InterProcessAggregator<Integer, Integer> aggregator) throws Exception {
    for (int i = 0; i < number; ++i) {
      aggregator.aggregate(1);
    }
    aggregator.flush();
    for (int i = 0; i < number; ++i) {
      aggregator.aggregate(1);
    }
    aggregator.flush();
  }

  @Test
  public void testKeyedAggregator() throws Exception {
    Variable<ImmutableMap<String, ImmutableSet<Integer>>> variable = new InMemoryLocal<ImmutableMap<String, ImmutableSet<Integer>>>();
    Aggregator<Integer, ImmutableSet<Integer>> aggregator = new SetAggregator<Integer>();

    InterProcessKeyedAggregator<String, Integer, ImmutableSet<Integer>> aggregator1 = new InterProcessKeyedAggregator<String, Integer, ImmutableSet<Integer>>(variable, aggregator);
    InterProcessKeyedAggregator<String, Integer, ImmutableSet<Integer>> aggregator2 = new InterProcessKeyedAggregator<String, Integer, ImmutableSet<Integer>>(variable, aggregator);
    InterProcessKeyedAggregator<String, Integer, ImmutableSet<Integer>> aggregator3 = new InterProcessKeyedAggregator<String, Integer, ImmutableSet<Integer>>(variable, aggregator);

    aggregator1.aggregate("a", 1);
    aggregator1.aggregate("b", 1);
    aggregator1.aggregate("c", 1);
    aggregator3.aggregate("e", 1);

    aggregator2.aggregate("a", 1);
    aggregator2.aggregate("d", 2);
    aggregator3.aggregate("e", 2);

    aggregator2.aggregate("a", 1);
    aggregator3.aggregate("c", 3);
    aggregator3.aggregate("d", 3);
    aggregator3.aggregate("e", 3);

    aggregator1.flush();
    aggregator2.flush();
    aggregator3.flush();

    assertEquals(ImmutableSet.of(1), aggregator1.read("a"));
    assertEquals(ImmutableSet.of(1), aggregator1.read("b"));
    assertEquals(ImmutableSet.of(1, 3), aggregator1.read("c"));
    assertEquals(ImmutableSet.of(2, 3), aggregator1.read("d"));
    assertEquals(ImmutableSet.of(1, 2, 3), aggregator1.read("e"));
  }

  @Test
  public void testKeyedAttemptsAggregator() throws Exception {
    Variable<ImmutableMap<Integer, ImmutableMap<String, Integer>>> variable = new InMemoryLocal<ImmutableMap<Integer, ImmutableMap<String, Integer>>>();
    Aggregator<Integer, Integer> aggregator = new SumAggregator();

    Integer attempt1 = 1;
    Integer attempt2 = 2;

    InterProcessKeyedAttemptsAggregator<Integer, String, Integer, Integer> aggregator1 = new InterProcessKeyedAttemptsAggregator<Integer, String, Integer, Integer>(attempt1, variable, aggregator);
    InterProcessKeyedAttemptsAggregator<Integer, String, Integer, Integer> aggregator2a = new InterProcessKeyedAttemptsAggregator<Integer, String, Integer, Integer>(attempt2, variable, aggregator);
    InterProcessKeyedAttemptsAggregator<Integer, String, Integer, Integer> aggregator2b = new InterProcessKeyedAttemptsAggregator<Integer, String, Integer, Integer>(attempt2, variable, aggregator);

    aggregator1.aggregate("a", 1);

    aggregator2a.aggregate("a", 1);
    aggregator2a.aggregate("c", 3);

    aggregator2b.aggregate("a", 1);
    aggregator2b.aggregate("b", 1);

    aggregator1.flush();
    aggregator2a.flush();
    aggregator2b.flush();

    assertEquals(Integer.valueOf(2), aggregator1.read("a"));
    assertEquals(null, aggregator1.read("b"));
    assertEquals(Integer.valueOf(3), aggregator1.read("c"));
  }
}
