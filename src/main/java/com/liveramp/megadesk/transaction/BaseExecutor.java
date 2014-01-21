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
  public <V> V call(Function<V> function) throws Exception {
    execute(function);
    return function.result().persistence().get();
  }

  @Override
  public <V> ExecutionResult<V> tryCall(Function<V> function) throws Exception {
    if (tryExecute(function)) {
      return new ExecutionResult<V>(true, function.result().persistence().get());
    } else {
      return new ExecutionResult<V>(false, null);
    }
  }
}
