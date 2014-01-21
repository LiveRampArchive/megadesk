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
  public void execute(Procedure procedure) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Transaction transaction = transactionExecution.begin(procedure.dependency());
    try {
      procedure.run(transaction);
      transactionExecution.commit();
    } catch (Exception e) {
      transactionExecution.abort();
      throw e;
    }
  }

  @Override
  public boolean tryExecute(Procedure procedure) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Transaction transaction = transactionExecution.tryBegin(procedure.dependency());
    if (transaction != null) {
      try {
        procedure.run(transaction);
        transactionExecution.commit();
        return true;
      } catch (Exception e) {
        transactionExecution.abort();
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
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Transaction transaction = transactionExecution.begin(makeFunctionDependency(function.dependency(), result));
    try {
      Value<V> resultValue = function.call(transaction);
      // Write result only if needed
      if (result != null) {
        transaction.write(result.reference(), resultValue);
      }
      transactionExecution.commit();
      return resultValue;
    } catch (Exception e) {
      transactionExecution.abort();
      throw e;
    }
  }

  @Override
  public <V> ExecutionResult<Value<V>> tryExecute(Function<V> function, Driver<V> result) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Transaction transaction = transactionExecution.tryBegin(makeFunctionDependency(function.dependency(), result));
    if (transaction != null) {
      try {
        Value<V> resultValue = function.call(transaction);
        // Write result only if needed
        if (result != null) {
          transaction.write(result.reference(), resultValue);
        }
        transactionExecution.commit();
        return new ExecutionResult<Value<V>>(true, resultValue);
      } catch (Exception e) {
        transactionExecution.abort();
        throw e;
      }
    } else {
      return new ExecutionResult<Value<V>>(false, null);
    }
  }

  private static Dependency<Driver> makeFunctionDependency(Dependency<Driver> dependency, Driver resultValue) {
    if (resultValue != null) {
      Dependency<Driver> result;
      // Original dependency, with result added as a write
      List<Driver> writes = Lists.newArrayList(dependency.writes());
      if (!writes.contains(resultValue)) {
        writes.add(resultValue);
      }
      result = BaseDependency.<Driver>builder()
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
