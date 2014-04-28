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

import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.recipes.transaction.Alter;
import com.liveramp.megadesk.recipes.transaction.Read;
import com.liveramp.megadesk.recipes.transaction.Write;

public class InterProcessAggregator<AGGREGAND, AGGREGATE> {

  private final Aggregator<AGGREGAND, AGGREGATE> aggregator;
  private final Variable<AGGREGATE> variable;
  private final TransactionExecutor executor;
  private AGGREGATE aggregate;

  public InterProcessAggregator(Variable<AGGREGATE> variable, Aggregator<AGGREGAND, AGGREGATE> aggregator) {
    this.variable = variable;
    this.aggregator = aggregator;
    this.executor = new BaseTransactionExecutor();
    this.aggregate = aggregator.initialValue();
  }

  public void initialize() throws Exception {
    executor.execute(new Write<AGGREGATE>(variable, aggregator.initialValue()));
  }

  public AGGREGATE aggregate(AGGREGAND value) {
    aggregate = aggregator.aggregate(value, aggregate);
    return aggregate;
  }

  public AGGREGATE flush() throws Exception {
    AGGREGATE result = executor.execute(new Aggregate<AGGREGAND, AGGREGATE>(variable, aggregator, aggregate));
    this.aggregate = aggregator.initialValue();
    return result;
  }

  public AGGREGATE read() throws Exception {
    return executor.execute(new Read<AGGREGATE>(variable));
  }

  private static class Aggregate<AGGREGAND, AGGREGATE> extends Alter<AGGREGATE> {

    private final Aggregator<AGGREGAND, AGGREGATE> aggregator;
    private final AGGREGATE partialAggregate;

    private Aggregate(Variable<AGGREGATE> variable, Aggregator<AGGREGAND, AGGREGATE> aggregator, AGGREGATE partialAggregate) {
      super(variable);
      this.aggregator = aggregator;
      this.partialAggregate = partialAggregate;
    }

    @Override
    protected AGGREGATE alter(AGGREGATE aggregate) {
      if (aggregate == null) {
        aggregate = aggregator.initialValue();
      }
      return aggregator.merge(aggregate, partialAggregate);
    }
  }
}
