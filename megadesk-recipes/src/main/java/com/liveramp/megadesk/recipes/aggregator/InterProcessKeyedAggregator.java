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

public class InterProcessKeyedAggregator<KEY, AGGREGAND, AGGREGATE>
    implements InterProcessKeyedAggregatorInterface<KEY, AGGREGAND, AGGREGATE> {

  private final InterProcessAggregator<KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<KEY, AGGREGATE>> innerAggregator;

  public InterProcessKeyedAggregator(Variable<ImmutableMap<KEY, AGGREGATE>> variable,
                                     Aggregator<AGGREGAND, AGGREGATE> aggregator) {
    this.innerAggregator = new InterProcessAggregator<KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<KEY, AGGREGATE>>(variable, new KeyedAggregator<KEY, AGGREGAND, AGGREGATE>(aggregator));
  }

  @Override
  public void initialize() throws Exception {
    innerAggregator.initialize();
  }

  @Override
  public AGGREGATE aggregate(KEY key, AGGREGAND value) {
    return innerAggregator.aggregate(new KeyAndAggregand<KEY, AGGREGAND>(key, value)).get(key);
  }

  @Override
  public void flush() throws Exception {
    innerAggregator.flush();
  }

  @Override
  public AGGREGATE read(KEY key) throws Exception {
    ImmutableMap<KEY, AGGREGATE> remote = innerAggregator.read();
    if (remote == null) {
      return null;
    } else {
      return remote.get(key);
    }
  }

  @Override
  public ImmutableMap<KEY, AGGREGATE> read() throws Exception {
    return innerAggregator.read();
  }

  private static class KeyedAggregator<KEY, AGGREGAND, AGGREGATE>
      implements Aggregator<KeyAndAggregand<KEY, AGGREGAND>, ImmutableMap<KEY, AGGREGATE>> {

    private final Aggregator<AGGREGAND, AGGREGATE> aggregator;

    private KeyedAggregator(Aggregator<AGGREGAND, AGGREGATE> aggregator) {
      this.aggregator = aggregator;
    }

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
      Map<KEY, AGGREGATE> result = Maps.newHashMap(lhs);
      mergeMap(rhs, result);
      return ImmutableMap.copyOf(result);
    }

    private void mergeMap(ImmutableMap<KEY, AGGREGATE> input, Map<KEY, AGGREGATE> result) {
      for (Map.Entry<KEY, AGGREGATE> entry : input.entrySet()) {
        KEY key = entry.getKey();
        AGGREGATE aggregate = entry.getValue();
        if (!result.containsKey(key)) {
          result.put(key, aggregator.initialValue());
        }
        result.put(key, aggregator.merge(aggregate, result.get(key)));
      }
    }
  }
}
