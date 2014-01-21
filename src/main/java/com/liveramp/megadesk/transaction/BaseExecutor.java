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

public class BaseExecutor implements Executor {

  @Override
  public void execute(Method method) throws Exception {
    Transaction transaction = new BaseTransaction();
    TransactionData transactionData = transaction.begin(method.dependency());
    try {
      method.run(transactionData);
      transaction.commit();
    } catch (Exception e) {
      transaction.abort();
      throw e;
    }
  }

  @Override
  public boolean tryExecute(Method method) throws Exception {
    Transaction transaction = new BaseTransaction();
    TransactionData transactionData = transaction.tryBegin(method.dependency());
    if (transactionData != null) {
      try {
        method.run(transactionData);
        transaction.commit();
        return true;
      } catch (Exception e) {
        transaction.abort();
        throw e;
      }
    } else {
      return false;
    }
  }

  @Override
  public <V> Value<V> execute(Function<V> function) throws Exception {
    return execute(function, null);
  }

  @Override
  public <V> ExecutionResult<Value<V>> tryExecute(Function<V> function) throws Exception {
    return tryExecute(function, null);
  }

  @Override
  public <V> Value<V> execute(Function<V> function, Driver<V> result) throws Exception {
    Transaction transaction = new BaseTransaction();
    TransactionData transactionData = transaction.begin(makeFunctionDependency(function.dependency(), result));
    try {
      Value<V> resultValue = function.call(transactionData);
      // Write result only if needed
      if (result != null) {
        transactionData.write(result.reference(), resultValue);
      }
      transaction.commit();
      return resultValue;
    } catch (Exception e) {
      transaction.abort();
      throw e;
    }
  }

  @Override
  public <V> ExecutionResult<Value<V>> tryExecute(Function<V> function, Driver<V> result) throws Exception {
    Transaction transaction = new BaseTransaction();
    TransactionData transactionData = transaction.tryBegin(makeFunctionDependency(function.dependency(), result));
    if (transactionData != null) {
      try {
        Value<V> resultValue = function.call(transactionData);
        // Write result only if needed
        if (result != null) {
          transactionData.write(result.reference(), resultValue);
        }
        transaction.commit();
        return new ExecutionResult<Value<V>>(true, resultValue);
      } catch (Exception e) {
        transaction.abort();
        throw e;
      }
    } else {
      return new ExecutionResult<Value<V>>(false, null);
    }
  }

  private static TransactionDependency makeFunctionDependency(TransactionDependency dependency, Driver resultValue) {
    if (resultValue != null) {
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
    } else {
      // If not writing the return value, no change to the dependency
      return dependency;
    }
  }
}
