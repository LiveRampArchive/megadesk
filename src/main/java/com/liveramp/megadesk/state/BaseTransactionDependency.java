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

package com.liveramp.megadesk.state;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

public class BaseTransactionDependency implements TransactionDependency {

  private Map<Reference, Driver> reads;
  private Map<Reference, Driver> writes;

  public BaseTransactionDependency(Collection<Driver> reads,
                                   Collection<Driver> writes) {
    this.reads = Collections.unmodifiableMap(map(reads));
    this.writes = Collections.unmodifiableMap(map(writes));
  }

  @Override
  public Collection<Driver> reads() {
    return reads.values();
  }

  @Override
  public Collection<Driver> writes() {
    return writes.values();
  }

  private Map<Reference, Driver> map(Collection<Driver> drivers) {
    Map<Reference, Driver> result = Maps.newHashMap();
    for (Driver driver : drivers) {
      result.put(driver.reference(), driver);
    }
    return result;
  }

  public Set<Reference> readReferences() {
    return reads.keySet();
  }

  public Set<Reference> writeReferences() {
    return writes.keySet();
  }

  public <VALUE> Driver<VALUE> readDriver(Reference<VALUE> reference) {
    return reads.get(reference);
  }

  public <VALUE> Driver<VALUE> writeDriver(Reference<VALUE> reference) {
    return writes.get(reference);
  }

  public static class Builder {

    private List<Driver> reads;
    private List<Driver> writes;

    public Builder reads(Driver... references) {
      return reads(Arrays.asList(references));
    }

    public Builder reads(List<Driver> references) {
      this.reads = Collections.unmodifiableList(references);
      return this;
    }

    public Builder writes(Driver... references) {
      return writes(Arrays.asList(references));
    }

    public Builder writes(List<Driver> references) {
      this.writes = Collections.unmodifiableList(references);
      return this;
    }

    public BaseTransactionDependency build() {
      return new BaseTransactionDependency(reads, writes);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
