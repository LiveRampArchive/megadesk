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

package com.liveramp.megadesk.base.state;

import com.liveramp.megadesk.core.state.MultiPersistenceTransaction;
import com.liveramp.megadesk.core.state.Persistence;
import com.liveramp.megadesk.core.state.PersistenceTransaction;

public abstract class BasePersistence<VALUE> implements Persistence<VALUE> {

  @Override
  public void writeInMultiTransaction(MultiPersistenceTransaction transaction, VALUE value) {
    PersistenceTransaction persistenceTransaction = transaction.getTransactionFor(transactionCategory());
    if (persistenceTransaction == null) {
      persistenceTransaction = newTransaction();
      transaction.startTransactionFor(transactionCategory(), persistenceTransaction);
    }
    writeInTransaction(persistenceTransaction, value);
  }

  public abstract Object transactionCategory();

  public abstract PersistenceTransaction newTransaction();

  public abstract void writeInTransaction(PersistenceTransaction transaction, VALUE value);
}
