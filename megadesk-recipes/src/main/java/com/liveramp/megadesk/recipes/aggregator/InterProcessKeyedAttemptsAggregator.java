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

public class InterProcessKeyedAttemptsAggregator<ATTEMPT, KEY, AGGREGAND, AGGREGATE>
    implements InterProcessKeyedAggregatorInterface<KEY, AGGREGAND, AGGREGATE> {

  private final ATTEMPT attempt;
  private final Aggregator<AGGREGAND, AGGREGATE> aggregator;
  private final InterProcessKeyedAggregator<KEY, AGGREGAND, ImmutableMap<ATTEMPT, AGGREGATE>> innerAggregator;

  public InterProcessKeyedAttemptsAggregator(ATTEMPT attempt, Variable<ImmutableMap<KEY, ImmutableMap<ATTEMPT, AGGREGATE>>> variable, Aggregator<AGGREGAND, AGGREGATE> aggregator) {
    this.attempt = attempt;
    this.aggregator = aggregator;
    innerAggregator = new InterProcessKeyedAggregator<KEY, AGGREGAND, ImmutableMap<ATTEMPT, AGGREGATE>>(variable, new AttemptedKeyedAggregator());
  }

  @Override
  public void initialize() throws Exception {
    innerAggregator.initialize();
  }

  @Override
  public AGGREGATE aggregate(KEY key, AGGREGAND value) {
    return aggregateAttempts(innerAggregator.aggregate(key, value));
  }

  @Override
  public void flush() throws Exception {
    innerAggregator.flush();
  }

  @Override
  public AGGREGATE read(KEY key) throws Exception {
    return aggregateAttempts(innerAggregator.read(key));
  }

  private AGGREGATE aggregateAttempts(Map<ATTEMPT, AGGREGATE> attempts) {
    // Aggregate attempts on the fly
    AGGREGATE result = aggregator.initialValue();
    for (Map.Entry<ATTEMPT, AGGREGATE> entry : attempts.entrySet()) {
      result = aggregator.merge(result, entry.getValue());
    }
    return result;
  }

  private class AttemptedKeyedAggregator implements Aggregator<AGGREGAND, ImmutableMap<ATTEMPT, AGGREGATE>> {

    @Override
    public ImmutableMap<ATTEMPT, AGGREGATE> initialValue() {
      return ImmutableMap.of();
    }

    @Override
    public ImmutableMap<ATTEMPT, AGGREGATE> aggregate(AGGREGAND value, ImmutableMap<ATTEMPT, AGGREGATE> aggregate) {
      Map<ATTEMPT, AGGREGATE> result = Maps.newHashMap(aggregate);
      if (!result.containsKey(attempt)) {
        result.put(attempt, aggregator.initialValue());
      }
      result.put(attempt, aggregator.aggregate(value, result.get(attempt)));
      return ImmutableMap.copyOf(result);
    }

    @Override
    public ImmutableMap<ATTEMPT, AGGREGATE> merge(ImmutableMap<ATTEMPT, AGGREGATE> lhs, ImmutableMap<ATTEMPT, AGGREGATE> rhs) {
      Map<ATTEMPT, AGGREGATE> result = Maps.newHashMap(lhs);
      for (Map.Entry<ATTEMPT, AGGREGATE> entry : rhs.entrySet()) {
        // Add only attempts that are missing
        if (!result.containsKey(entry.getKey())) {
          result.put(entry.getKey(), entry.getValue());
        }
      }
      return ImmutableMap.copyOf(result);
    }
  }
}
