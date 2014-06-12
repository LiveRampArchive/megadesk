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

package com.liveramp.megadesk.recipes.transaction;

import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseTransaction;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Transaction;

public class Composition extends BaseTransaction<Void> implements Transaction<Void> {

  private final List<Transaction> transactions;

  public Composition(Transaction... transactions) {
    super(composedDependency(Arrays.asList(transactions)));
    this.transactions = Arrays.asList(transactions);
  }

  @Override
  public Void run(Context context) throws Exception {
    for (Transaction transaction : transactions) {
      transaction.run(context);
    }
    return null;
  }

  private static Dependency composedDependency(List<Transaction> transactions) {
    List<Dependency> dependencies = Lists.newArrayList();
    for (Transaction transaction : transactions) {
      dependencies.add(transaction.dependency());
    }
    return BaseDependency.merge(dependencies.toArray(new Dependency[dependencies.size()]));
  }
}
