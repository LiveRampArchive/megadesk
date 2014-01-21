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

import com.google.common.collect.Lists;

import com.liveramp.megadesk.state.Driver;

public class BaseTransactionDependency implements TransactionDependency {

  private final List<Driver> snapshots;
  private final List<Driver> reads;
  private final List<Driver> writes;

  public BaseTransactionDependency(Collection<Driver> snapshots,
                                   Collection<Driver> reads,
                                   Collection<Driver> writes) {
    this.snapshots = prepareList(snapshots);
    this.reads = prepareList(reads);
    this.writes = prepareList(writes);
    checkIntegrity();
  }

  private List<Driver> prepareList(Collection<Driver> drivers) {
    // Deep copy, sort, and make unmodifiable
    List<Driver> result = Lists.newArrayList(drivers);
    Collections.sort(result);
    return Collections.unmodifiableList(result);
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
  public List<Driver> snapshots() {
    return snapshots;
  }

  @Override
  public List<Driver> reads() {
    return reads;
  }

  @Override
  public List<Driver> writes() {
    return writes;
  }

  public static class Builder {

    private List<Driver> snapshots = Collections.emptyList();
    private List<Driver> reads = Collections.emptyList();
    private List<Driver> writes = Collections.emptyList();

    public Builder snapshots(Driver... drivers) {
      return snapshots(Arrays.asList(drivers));
    }

    public Builder snapshots(List<Driver>... lists) {
      return snapshots(concatenate(lists));
    }

    public Builder snapshots(List<Driver> drivers) {
      this.snapshots = Lists.newArrayList(drivers);
      return this;
    }

    public Builder reads(Driver... drivers) {
      return reads(Arrays.asList(drivers));
    }

    public Builder reads(List<Driver>... lists) {
      return reads(concatenate(lists));
    }

    public Builder reads(List<Driver> drivers) {
      this.reads = Lists.newArrayList(drivers);
      return this;
    }

    public Builder writes(Driver... drivers) {
      return writes(Arrays.asList(drivers));
    }

    public Builder writes(List<Driver>... lists) {
      return writes(concatenate(lists));
    }

    public Builder writes(List<Driver> drivers) {
      this.writes = Lists.newArrayList(drivers);
      return this;
    }

    private List<Driver> concatenate(List<Driver>... lists) {
      List<Driver> result = Lists.newArrayList();
      for (List<Driver> list : lists) {
        result.addAll(list);
      }
      return result;
    }


    public BaseTransactionDependency build() {
      return new BaseTransactionDependency(snapshots, reads, writes);
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
