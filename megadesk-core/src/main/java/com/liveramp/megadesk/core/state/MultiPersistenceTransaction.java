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

package com.liveramp.megadesk.core.state;

import java.util.Map;

import com.google.common.collect.Maps;

public class MultiPersistenceTransaction {

  private final Map<Object, PersistenceTransaction> transactions;

  public MultiPersistenceTransaction() {
    transactions = Maps.newHashMap();
  }

  public boolean containsTransactionFor(Object object) {
    return transactions.containsKey(object);
  }

  public PersistenceTransaction getTransactionFor(Object object) {
    return transactions.get(object);
  }

  public void startTransactionFor(Object object, PersistenceTransaction transaction) {
    transactions.put(object, transaction);
  }

  public void commit() {
    for (PersistenceTransaction transaction : transactions.values()) {
      transaction.commit();
    }
  }
}
