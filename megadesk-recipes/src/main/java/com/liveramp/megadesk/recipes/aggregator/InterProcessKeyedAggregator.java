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

public class InterProcessKeyedAggregator<KEY, AGGREGATE> extends InterProcessAggregator<ImmutableMap<KEY, AGGREGATE>> {

  public InterProcessKeyedAggregator(Variable<ImmutableMap<KEY, AGGREGATE>> variable,
                                     Aggregator<AGGREGATE> aggregator) {
    super(variable, new KeyedAggregator<KEY, AGGREGATE>(aggregator));
  }

  public AGGREGATE aggregateLocal(KEY key, AGGREGATE value) {
    ImmutableMap<KEY, AGGREGATE> aggregate = ImmutableMap.of(key, value);
    return aggregateLocal(aggregate).get(key);
  }

  public AGGREGATE readLocal(KEY key) {
    return readLocal().get(key);
  }

  public AGGREGATE readRemote(KEY key) throws Exception {
    ImmutableMap<KEY, AGGREGATE> remote = readRemote();
    if (remote == null) {
      return null;
    } else {
      return remote.get(key);
    }
  }

  private static class KeyedAggregator<KEY, AGGREGATE> implements Aggregator<ImmutableMap<KEY, AGGREGATE>> {

    private final Aggregator<AGGREGATE> aggregator;

    private KeyedAggregator(Aggregator<AGGREGATE> aggregator) {
      this.aggregator = aggregator;
    }

    @Override
    public ImmutableMap<KEY, AGGREGATE> initialValue() {
      return ImmutableMap.of();
    }

    @Override
    public ImmutableMap<KEY, AGGREGATE> aggregate(ImmutableMap<KEY, AGGREGATE> aggregate, ImmutableMap<KEY, AGGREGATE> value) {
      Map<KEY, AGGREGATE> result = Maps.newHashMap();
      aggregateMap(aggregate, result);
      aggregateMap(value, result);
      return ImmutableMap.copyOf(result);
    }

    private void aggregateMap(ImmutableMap<KEY, AGGREGATE> input, Map<KEY, AGGREGATE> result) {
      for (Map.Entry<KEY, AGGREGATE> entry : input.entrySet()) {
        KEY key = entry.getKey();
        AGGREGATE aggregate = entry.getValue();
        if (!result.containsKey(key)) {
          result.put(key, aggregator.initialValue());
        }
        result.put(key, aggregator.aggregate(result.get(key), aggregate));
      }
    }
  }
}
