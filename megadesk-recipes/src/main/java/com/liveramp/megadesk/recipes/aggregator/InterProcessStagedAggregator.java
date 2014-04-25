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
package com.liveramp.megadesk.recipes.aggregator;

import java.io.Serializable;

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Transaction;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.recipes.transaction.Alter;
import com.liveramp.megadesk.recipes.transaction.Read;
import com.liveramp.megadesk.recipes.transaction.Write;

public class InterProcessStagedAggregator<Aggregate extends Serializable, Value> {

  private final VariableProvider<Aggregate> variableProvider;
  private final ExecutorFactory executorFactory;
  private final Aggregator<Aggregate, Value> aggregator;
  private Aggregate cachedValue;

  private transient Variable<Aggregate> variable;
  private transient TransactionExecutor executor;

  public InterProcessStagedAggregator(VariableProvider<Aggregate> variableProvider, ExecutorFactory executorFactory, Aggregator<Aggregate, Value> aggregator) {
    this.variableProvider = variableProvider;
    this.executorFactory = executorFactory;
    this.aggregator = aggregator;
  }

  public void prepare() {
    Write write = new Write(getVariable(), aggregator.initialValue());
    exec(write);
  }

  public Aggregate updateLocal(Value newValue) {
    if (cachedValue == null) {
      cachedValue = aggregator.initialValue();
    }
    cachedValue = aggregator.partialAggregate(cachedValue, newValue);
    return cachedValue;
  }

  public Aggregate readLocal() {
    return cachedValue;
  }

  public Aggregate updateRemote() {
    Update<Aggregate> update = new Update<Aggregate>(getVariable(), aggregator, cachedValue);
    Aggregate aggregate = exec(update);
    cachedValue = aggregator.initialValue();
    return aggregate;
  }

  public Aggregate readRemote() {
    Read<Aggregate> read = new Read<Aggregate>(getVariable());
    return exec(read);
  }


  private <T> T exec(Transaction<T> t) {
    if (executor == null) {
      executor = executorFactory.create();
    }
    try {
      return executor.execute(t);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Variable<Aggregate> getVariable() {
    if (variable == null) {
      variable = variableProvider.getVariable();
    }
    return variable;
  }

  private static class Update<Aggregate> extends Alter<Aggregate> {

    private final Aggregator<Aggregate, ?> aggregator;
    private final Aggregate partialAggregate;

    private Update(Variable<Aggregate> variable, Aggregator<Aggregate, ?> aggregator, Aggregate partialAggregate) {
      super(variable);
      this.aggregator = aggregator;
      this.partialAggregate = partialAggregate;
    }

    @Override
    protected Aggregate alter(Aggregate aggregate) {
      return aggregator.finalAggregate(aggregate, partialAggregate);
    }
  }


}
