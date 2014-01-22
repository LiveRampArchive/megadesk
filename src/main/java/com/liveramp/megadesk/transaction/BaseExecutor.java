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
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.lib.InMemoryValue;

public class BaseExecutor implements Executor {

  @Override
  public <V> V execute(UnboundProcedure<V> procedure, Driver... arguments) throws Exception {
    return execute(new BoundProcedure<V>(new ProcedureCall<V>(procedure, Arrays.asList(arguments))));
  }

  @Override
  public <V> V execute(Procedure<V> procedure) throws Exception {
    return execute(procedure, null);
  }

  @Override
  public <V> ExecutionResult<V> tryExecute(Procedure<V> procedure) throws Exception {
    return tryExecute(procedure, null);
  }

  @Override
  public <V> V execute(Procedure<V> procedure, Driver<V> result) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Transaction transaction = transactionExecution.begin(makeFunctionDependency(procedure.dependency(), result));
    try {
      V resultValue = procedure.run(transaction);
      // Write result only if needed
      if (result != null) {
        transaction.write(result.reference(), new InMemoryValue<V>(resultValue));
      }
      transactionExecution.commit();
      return resultValue;
    } catch (Exception e) {
      transactionExecution.abort();
      throw e;
    }
  }

  @Override
  public <V> ExecutionResult<V> tryExecute(Procedure<V> procedure, Driver<V> result) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Transaction transaction = transactionExecution.tryBegin(makeFunctionDependency(procedure.dependency(), result));
    if (transaction != null) {
      try {
        V resultValue = procedure.run(transaction);
        // Write result only if needed
        if (result != null) {
          transaction.write(result.reference(), new InMemoryValue<V>(resultValue));
        }
        transactionExecution.commit();
        return new ExecutionResult<V>(true, resultValue);
      } catch (Exception e) {
        transactionExecution.abort();
        throw e;
      }
    } else {
      return new ExecutionResult<V>(false, null);
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
