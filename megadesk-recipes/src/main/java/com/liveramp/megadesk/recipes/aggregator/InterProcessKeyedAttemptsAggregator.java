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
import org.apache.commons.lang.NotImplementedException;

import com.liveramp.megadesk.core.state.Variable;

public class InterProcessKeyedAttemptsAggregator<ATTEMPT, KEY, AGGREGAND, AGGREGATE>
    implements InterProcessKeyedAggregatorInterface<KEY, AGGREGAND, AGGREGATE> {

  private final ATTEMPT attempt;
  private final Aggregator<AGGREGAND, AGGREGATE> aggregator;
  private final InterProcessKeyedAggregator<ATTEMPT, KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<KEY, AGGREGATE>> innerAggregator;

  public InterProcessKeyedAttemptsAggregator(ATTEMPT attempt, Variable<ImmutableMap<ATTEMPT, ImmutableMap<KEY, AGGREGATE>>> variable, Aggregator<AGGREGAND, AGGREGATE> aggregator) {
    this.attempt = attempt;
    this.aggregator = aggregator;
    innerAggregator = new InterProcessKeyedAggregator<ATTEMPT, KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<KEY, AGGREGATE>>(variable, new AttemptedKeyedAggregator());
  }

  @Override
  public void initialize() throws Exception {
    innerAggregator.initialize();
  }

  @Override
  public AGGREGATE aggregate(KEY key, AGGREGAND value) {
    return innerAggregator.aggregate(attempt, new KeyAndAggregand<KEY, AGGREGAND>(key, value)).get(key);
  }

  @Override
  public void flush() throws Exception {
    innerAggregator.flush();
  }

  @Override
  public AGGREGATE read(KEY key) throws Exception {
    return aggregateAttempts(key, innerAggregator.read());
  }

  @Override
  public ImmutableMap<KEY, AGGREGATE> read() throws Exception {
    // TODO
    throw new NotImplementedException();
  }

  private AGGREGATE aggregateAttempts(KEY key, ImmutableMap<ATTEMPT, ImmutableMap<KEY, AGGREGATE>> attempts) {
    // Aggregate attempts on the fly
    AGGREGATE result = aggregator.initialValue();
    for (Map.Entry<ATTEMPT, ImmutableMap<KEY, AGGREGATE>> entry : attempts.entrySet()) {
      if (entry.getValue().containsKey(key)) {
        result = aggregator.merge(result, entry.getValue().get(key));
      }
    }
    return result;
  }

  private class AttemptedKeyedAggregator implements Aggregator<KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<KEY, AGGREGATE>> {

    @Override
    public ImmutableMap<KEY, AGGREGATE> initialValue() {
      return ImmutableMap.of();
    }

    @Override
    public ImmutableMap<KEY, AGGREGATE> aggregate(KeyAndAggregand<KEY, AGGREGAND> value, ImmutableMap<KEY, AGGREGATE> aggregate) {
      Map<KEY, AGGREGATE> result = Maps.newHashMap(aggregate);
      if (!result.containsKey(value.getKey())) {
        result.put(value.getKey(), aggregator.initialValue());
      }
      result.put(value.getKey(), aggregator.aggregate(value.getAggregand(), result.get(value.getKey())));
      return ImmutableMap.copyOf(result);
    }

    @Override
    public ImmutableMap<KEY, AGGREGATE> merge(ImmutableMap<KEY, AGGREGATE> lhs, ImmutableMap<KEY, AGGREGATE> rhs) {
      // Keep only one attempt
      return lhs;
    }
  }
}
