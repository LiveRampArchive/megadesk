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

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.base.state.BaseVariable;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.Executor;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.core.transaction.TransactionExecution;

public class BaseExecutor implements Executor {

  @Override
  public <V> V execute(Transaction<V> transaction) throws Exception {
    return execute(transaction, null);
  }

  @Override
  public <V> ExecutionResult<V> tryExecute(Transaction<V> transaction) throws Exception {
    return tryExecute(transaction, null);
  }

  @Override
  public <V> V execute(Transaction<V> transaction, Binding binding) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Dependency dependency = bindDependency(transaction.dependency(), binding);
    Context context = transactionExecution.begin(dependency);
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
  public <V> ExecutionResult<V> tryExecute(Transaction<V> transaction, Binding binding) throws Exception {
    TransactionExecution transactionExecution = new BaseTransactionExecution();
    Dependency dependency = bindDependency(transaction.dependency(), binding);
    Context context = transactionExecution.tryBegin(dependency);
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

  private Dependency bindDependency(Dependency dependency, Binding binding) {
    List<Variable> snapshots = bindReferences(dependency.snapshots(), binding);
    List<Variable> reads = bindReferences(dependency.reads(), binding);
    List<Variable> writes = bindReferences(dependency.writes(), binding);
    List<Variable> commutations = bindReferences(dependency.commutations(), binding);
    // TODO check for extra bindings
    return BaseDependency.builder()
               .snapshots(snapshots)
               .reads(reads)
               .writes(writes)
               .commutations(commutations)
               .build();
  }

  private List<Variable> bindReferences(List<Variable> variables, Binding binding) {
    List<Variable> result = Lists.newArrayList();
    for (Variable variable : variables) {
      if (variable.driver() == null) {
        if (binding == null) {
          throw new IllegalStateException("Binding is null");
        }
        result.add(new BaseVariable(variable.reference(), binding.get(variable.reference())));
      } else {
        result.add(variable);
      }
    }
    return result;
  }
}
