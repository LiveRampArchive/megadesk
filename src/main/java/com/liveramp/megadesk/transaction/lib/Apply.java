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

package com.liveramp.megadesk.transaction.lib;

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.transaction.BaseTransactionDependency;
import com.liveramp.megadesk.transaction.Function;
import com.liveramp.megadesk.transaction.TransactionData;
import com.liveramp.megadesk.transaction.TransactionDependency;

public class Apply<V> implements Function<Value<V>> {

  private final Function<Value<V>> function;
  private final Reference<V> returnValue;
  private final TransactionDependency dependency;

  public Apply(Function<Value<V>> function, Driver<V> returnValue) {
    this.function = function;
    this.returnValue = returnValue.reference();
    // Original dependency, with result added as a write
    List<Driver> writes = Lists.newArrayList(function.dependency().writes());
    if (!writes.contains(returnValue)) {
      writes.add(returnValue);
    }
    this.dependency = BaseTransactionDependency.builder()
                          .snapshots(function.dependency().snapshots())
                          .reads(function.dependency().reads())
                          .writes(writes)
                          .build();
  }

  @Override
  public TransactionDependency dependency() {
    return dependency;
  }

  @Override
  public Value<V> run(TransactionData transactionData) throws Exception {
    Value<V> result = function.run(transactionData);
    transactionData.write(returnValue, result);
    return result;
  }
}
