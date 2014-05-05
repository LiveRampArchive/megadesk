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

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.liveramp.megadesk.core.state.Variable;

public class InterProcessMultiTaskKeyedAggregator<TASK, KEY, AGGREGAND, AGGREGATE>
    implements InterProcessKeyedAggregatorInterface<KEY, AGGREGAND, AGGREGATE> {

  private final TASK task;
  private final Aggregator<AGGREGAND, AGGREGATE> aggregator;
  private final InterProcessAggregator<KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>>> innerAggregator;

  public InterProcessMultiTaskKeyedAggregator(TASK task,
                                              Variable<ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>>> variable,
                                              Aggregator<AGGREGAND, AGGREGATE> aggregator) {
    this.task = task;
    this.aggregator = aggregator;
    innerAggregator = new InterProcessAggregator<
        KeyAndAggregand<KEY, AGGREGAND>,
        ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>>>(variable, new KeyedAttemptsAggregator());
  }

  @Override
  public void initialize() throws Exception {
    innerAggregator.initialize();
  }

  @Override
  public AGGREGATE aggregate(KEY key, AGGREGAND value) {
    return innerAggregator.aggregate(new KeyAndAggregand<KEY, AGGREGAND>(key, value)).get(task).get(key);
  }

  @Override
  public void flush() throws Exception {
    innerAggregator.flush();
  }

  @Override
  public AGGREGATE read(KEY key) throws Exception {
    return aggregateAttempts(key, innerAggregator.read());
  }

  private AGGREGATE aggregateAttempts(KEY key, ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>> attempts) {
    // Aggregate attempts on the fly
    AGGREGATE result = null;
    for (Map.Entry<TASK, ImmutableMap<KEY, AGGREGATE>> entry : attempts.entrySet()) {
      if (entry.getValue().containsKey(key)) {
        if (result == null) {
          result = aggregator.initialValue();
        }
        result = aggregator.merge(result, entry.getValue().get(key));
      }
    }
    return result;
  }

  private class KeyedAttemptsAggregator
      implements Aggregator<KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>>> {

    @Override
    public ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>> initialValue() {
      return ImmutableMap.of();
    }

    @Override
    public ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>>
    aggregate(KeyAndAggregand<KEY, AGGREGAND> value,
              ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>> aggregate) {
      Map<TASK, ImmutableMap<KEY, AGGREGATE>> result = Maps.newHashMap(aggregate);
      if (!result.containsKey(task)) {
        result.put(task, ImmutableMap.<KEY, AGGREGATE>of());
      }
      Map<KEY, AGGREGATE> innerResult = Maps.newHashMap(result.get(task));
      if (!innerResult.containsKey(value.getKey())) {
        innerResult.put(value.getKey(), aggregator.initialValue());
      }
      innerResult.put(value.getKey(), aggregator.aggregate(value.getAggregand(), innerResult.get(value.getKey())));
      result.put(task, ImmutableMap.copyOf(innerResult));
      return ImmutableMap.copyOf(result);
    }

    @Override
    public ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>>
    merge(ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>> lhs,
          ImmutableMap<TASK, ImmutableMap<KEY, AGGREGATE>> rhs) {
      // Copy lhs
      Map<TASK, ImmutableMap<KEY, AGGREGATE>> result = Maps.newHashMap(lhs);
      for (Map.Entry<TASK, ImmutableMap<KEY, AGGREGATE>> entry : rhs.entrySet()) {
        // Keep only attempts from rhs that were not in lhs
        if (!result.containsKey(entry.getKey())) {
          result.put(entry.getKey(), entry.getValue());
        }
      }
      return ImmutableMap.copyOf(result);
    }
  }
}
