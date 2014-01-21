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

import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.transaction.lib.Apply;

public class BaseExecutor implements Executor {

  @Override
  public <V> V execute(Function<V> function) throws Exception {
    Transaction transaction = new BaseTransaction();
    TransactionData transactionData = transaction.begin(function.dependency());
    try {
      V result = function.run(transactionData);
      transaction.commit();
      return result;
    } catch (Exception e) {
      transaction.abort();
      throw e;
    }
  }

  @Override
  public <V> ExecutionResult<V> tryExecute(Function<V> function) throws Exception {
    Transaction transaction = new BaseTransaction();
    TransactionData transactionData = transaction.tryBegin(function.dependency());
    if (transactionData != null) {
      try {
        V result = function.run(transactionData);
        transaction.commit();
        return new ExecutionResult<V>(true, result);
      } catch (Exception e) {
        transaction.abort();
        throw e;
      }
    } else {
      return new ExecutionResult<V>(false, null);
    }
  }

  @Override
  public <V> Value<V> apply(Function<Value<V>> function, Driver<V> result) throws Exception {
    return execute(new Apply<V>(function, result));
  }

  @Override
  public <V> ExecutionResult<Value<V>> tryApply(Function<Value<V>> function, Driver<V> result) throws Exception {
    return tryExecute(new Apply<V>(function, result));
  }
}
