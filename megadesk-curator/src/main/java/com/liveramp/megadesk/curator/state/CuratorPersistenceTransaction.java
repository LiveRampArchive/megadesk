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

package com.liveramp.megadesk.curator.state;

import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;

import com.liveramp.megadesk.core.state.PersistenceTransaction;

public class CuratorPersistenceTransaction implements PersistenceTransaction {

  private final CuratorTransaction transaction;

  public CuratorPersistenceTransaction(CuratorTransaction transaction) {
    this.transaction = transaction;
  }

  @Override
  public void commit() {
    try {
      ((CuratorTransactionFinal)transaction).commit();
    } catch (Exception e) {
      throw new RuntimeException(e); // TODO
    }
  }

  public CuratorTransaction transaction() {
    return transaction;
  }
}
