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

  private final Collection<Driver> snapshots;
  private final Collection<Driver> reads;
  private final Collection<Driver> writes;

  public BaseTransactionDependency(Collection<Driver> snapshots,
                                   Collection<Driver> reads,
                                   Collection<Driver> writes) {
    this.snapshots = Collections.unmodifiableCollection(snapshots);
    this.reads = Collections.unmodifiableCollection(reads);
    this.writes = Collections.unmodifiableCollection(writes);
    checkIntegrity();
  }

  private void checkIntegrity() {
    for (Driver driver : snapshots) {
      if (reads.contains(driver) || writes.contains(driver)) {
        throw new IllegalStateException(); // TODO message
      }
    }
    for (Driver driver : reads) {
      if (writes.contains(driver)) {
        throw new IllegalStateException(); // TODO message
      }
    }
  }

  @Override
  public Collection<Driver> snapshots() {
    return snapshots;
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

    private List<Driver> snapshots = Collections.emptyList();
    private List<Driver> reads = Collections.emptyList();
    private List<Driver> writes = Collections.emptyList();

    public Builder snapshots(Driver... references) {
      return snapshots(Arrays.asList(references));
    }

    public Builder snapshots(List<Driver> references) {
      this.snapshots = references;
      return this;
    }

    public Builder reads(Driver... references) {
      return reads(Arrays.asList(references));
    }

    public Builder reads(List<Driver> references) {
      this.reads = references;
      return this;
    }

    public Builder writes(Driver... references) {
      return writes(Arrays.asList(references));
    }

    public Builder writes(List<Driver> references) {
      this.writes = references;
      return this;
    }

    public BaseTransactionDependency build() {
      return new BaseTransactionDependency(snapshots, reads, writes);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
