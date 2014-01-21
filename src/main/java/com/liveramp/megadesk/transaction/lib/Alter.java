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

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.transaction.BaseTransactionDependency;
import com.liveramp.megadesk.transaction.Function;
import com.liveramp.megadesk.transaction.TransactionData;
import com.liveramp.megadesk.transaction.TransactionDependency;

public abstract class Alter<V> implements Function<Value<V>> {

  private final Reference<V> reference;
  private final BaseTransactionDependency dependency;

  public Alter(Driver<V> driver) {
    this.reference = driver.reference();
    this.dependency = BaseTransactionDependency.builder().writes(driver).build();
  }

  @Override
  public TransactionDependency dependency() {
    return dependency;
  }

  @Override
  public Value<V> run(TransactionData transactionData) throws Exception {
    Value<V> result = alter(transactionData.read(reference));
    transactionData.write(reference, result);
    return result;
  }

  public abstract Value<V> alter(Value<V> value);
}
