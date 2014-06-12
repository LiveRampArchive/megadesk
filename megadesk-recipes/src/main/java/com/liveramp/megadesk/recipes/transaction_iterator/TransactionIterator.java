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

package com.liveramp.megadesk.recipes.transaction_iterator;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseTransaction;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

public class TransactionIterator<VALUE> {

  private final Variable<VALUE> variable;
  private final Dependency dependency;

  public TransactionIterator(Variable<VALUE> variable) {
    this.variable = variable;
    this.dependency = BaseDependency.builder().writes(variable).build();
  }

  public Transaction<VALUE> currentTransaction(final Transaction<VALUE> nextTransaction) {
    return new BaseTransaction<VALUE>(mergedDependency(nextTransaction.dependency())) {
      @Override
      public VALUE run(Context context) throws Exception {
        VALUE value = context.read(variable);
        if (value != null) {
          return value;
        } else {
          value = nextTransaction.run(context);
          context.write(variable, value);
          return value;
        }
      }
    };
  }

  public Transaction<VALUE> nextTransaction(final Transaction<VALUE> nextTransaction) {
    return new BaseTransaction<VALUE>(mergedDependency(nextTransaction.dependency())) {
      @Override
      public VALUE run(Context context) throws Exception {
        VALUE value = nextTransaction.run(context);
        context.write(variable, value);
        return value;
      }
    };
  }

  public VALUE current(Transaction<VALUE> nextTransaction) throws Exception {
    return new BaseTransactionExecutor().execute(currentTransaction(nextTransaction));
  }

  public VALUE next(Transaction<VALUE> nextTransaction) throws Exception {
    return new BaseTransactionExecutor().execute(nextTransaction(nextTransaction));
  }

  private Dependency mergedDependency(Dependency other) {
    return BaseDependency.merge(other, dependency);
  }
}
