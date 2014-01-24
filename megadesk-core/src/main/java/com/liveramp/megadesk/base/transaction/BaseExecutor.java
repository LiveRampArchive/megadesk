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

package com.liveramp.megadesk.base.transaction;

import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Executor;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.core.transaction.TransactionExecution;

public class BaseExecutor implements Executor {

  @Override
  public <V> V execute(Transaction<V> transaction) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Context context = transactionExecution.begin(transaction.dependency());
    try {
      V resultValue = transaction.run(context);
      transactionExecution.commit();
      return resultValue;
    } catch (Exception e) {
      transactionExecution.abort();
      throw e;
    }
  }

  @Override
  public <V> ExecutionResult<V> tryExecute(Transaction<V> transaction) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Context context = transactionExecution.tryBegin(transaction.dependency());
    if (context != null) {
      try {
        V resultValue = transaction.run(context);
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
}
