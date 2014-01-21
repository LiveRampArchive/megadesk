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


import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Value;

public abstract class BaseFunction<V> implements Function<V> {

  private TransactionDependency dependency;
  private final Driver<V> result;

  public BaseFunction(Driver<V> result) {
    this.result = result;
  }

  public BaseFunction(TransactionDependency dependency, Driver<V> result) {
    this.result = result;
    this.dependency = makeDependency(dependency, result);
  }

  @Override
  public final void run(TransactionData transactionData) throws Exception {
    Value<V> result = call(transactionData);
    transactionData.write(this.result().reference(), result);
  }

  @Override
  public final TransactionDependency dependency() {
    if (dependency == null) {
      throw new IllegalStateException(); // TODO message
    }
    return dependency;
  }

  @Override
  public Driver<V> result() {
    return result;
  }

  protected void setDependency(TransactionDependency dependency) {
    this.dependency = makeDependency(dependency, result());
  }

  private static TransactionDependency makeDependency(TransactionDependency dependency, Driver resultValue) {
    TransactionDependency result;
    // Original dependency, with result added as a write
    List<Driver> writes = Lists.newArrayList(dependency.writes());
    if (!writes.contains(resultValue)) {
      writes.add(resultValue);
    }
    result = BaseTransactionDependency.builder()
                 .snapshots(dependency.snapshots())
                 .reads(dependency.reads())
                 .writes(writes)
                 .build();
    return result;
  }
}
