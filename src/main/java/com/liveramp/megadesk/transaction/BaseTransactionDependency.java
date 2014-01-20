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

package com.liveramp.megadesk.transaction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.liveramp.megadesk.state.Driver;

public class BaseTransactionDependency implements TransactionDependency {

  private Collection<Driver> reads;
  private Collection<Driver> writes;

  public BaseTransactionDependency(Collection<Driver> reads,
                                   Collection<Driver> writes) {
    this.reads = Collections.unmodifiableCollection(reads);
    this.writes = Collections.unmodifiableCollection(writes);
  }

  @Override
  public Collection<Driver> reads() {
    return reads;
  }

  @Override
  public Collection<Driver> writes() {
    return writes;
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
