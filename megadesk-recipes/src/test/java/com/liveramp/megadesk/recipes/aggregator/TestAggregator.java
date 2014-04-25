package com.liveramp.megadesk.recipes.aggregator;

import junit.framework.Assert;
import org.junit.Test;

import com.liveramp.megadesk.base.state.InMemoryLocal;
import com.liveramp.megadesk.base.transaction.BaseTransactionExecutor;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.TransactionExecutor;
import com.liveramp.megadesk.test.BaseTestCase;

public class TestAggregator extends BaseTestCase {


  @Test
  public void testAggregators() throws InterruptedException {
    final Variable<Integer> var = new InMemoryLocal<Integer>();
    final TransactionExecutor exec = new BaseTransactionExecutor();
    VariableProvider<Integer> provider = new VariableProvider<Integer>() {
      @Override
      public Variable<Integer> getVariable() {
        return var;
      }
    };

    ExecutorFactory factory = new ExecutorFactory() {
      @Override
      public TransactionExecutor create() {
        return exec;
      }
    };


    Aggregator<Integer, Integer> sumAggregator = new Aggregator<Integer, Integer>() {
      @Override
      public Integer initialValue() {
        return 0;
      }

      @Override
      public Integer partialAggregate(Integer integer, Integer newValue) {
        return integer + newValue;
      }

      @Override
      public Integer finalAggregate(Integer finalAggregate, Integer partialAggregate) {
        return finalAggregate + partialAggregate;
      }
    };

    InterProcessStagedAggregator<Integer, Integer> aggregator =
        new InterProcessStagedAggregator<Integer, Integer>(provider, factory, sumAggregator);

    aggregator.prepare();

    Thread thread1 = makeThread(10, provider, factory, sumAggregator);
    Thread thread2 = makeThread(3, provider, factory, sumAggregator);
    Thread thread3 = makeThread(5, provider, factory, sumAggregator);

    thread1.start();
    thread2.start();
    thread3.start();

    thread1.join();
    thread2.join();
    thread3.join();

    Assert.assertEquals(Integer.valueOf(36), aggregator.readRemote());


  }

  private Thread makeThread(
      final int amount,
      final VariableProvider<Integer> provider,
      final ExecutorFactory factory,
      final Aggregator<Integer, Integer> sumAggregator) {

    final InterProcessStagedAggregator<Integer, Integer> aggregator =
        new InterProcessStagedAggregator<Integer, Integer>(provider, factory, sumAggregator);

    Runnable runnable = new Runnable() {

      @Override
      public void run() {
        for (int i = 0; i < amount; i++) {
          aggregator.updateLocal(1);
        }
        aggregator.updateRemote();
        for (int i = 0; i < amount; i++) {
          aggregator.updateLocal(1);
        }
        aggregator.updateRemote();
      }
    };

    return new Thread(runnable);


  }


}
