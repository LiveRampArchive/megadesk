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

package com.liveramp.megadesk;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.liveramp.megadesk.gear.ConditionalGear;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Outcome;
import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.state.lib.InMemoryDriver;
import com.liveramp.megadesk.state.lib.InMemoryValue;
import com.liveramp.megadesk.test.BaseTestCase;
import com.liveramp.megadesk.transaction.BaseExecutor;
import com.liveramp.megadesk.transaction.BaseTransactionDependency;
import com.liveramp.megadesk.transaction.Binding;
import com.liveramp.megadesk.transaction.Function;
import com.liveramp.megadesk.transaction.TransactionData;
import com.liveramp.megadesk.transaction.TransactionDependency;
import com.liveramp.megadesk.worker.NaiveWorker;
import com.liveramp.megadesk.worker.Worker;

import static org.junit.Assert.assertEquals;

public class IntegrationTest extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

  private TestingServer testingServer;

  @Before
  public void setUpDriver() throws Exception {
    this.testingServer = new TestingServer(12000);
  }

  @After
  public void tearDownDriver() throws IOException {
    this.testingServer.close();
  }

  private static class StepGear extends ConditionalGear implements Gear {

    private final List<StepGear> parents;
    private final Driver<Boolean> driver = new InMemoryDriver<Boolean>(new InMemoryValue<Boolean>(false));

    public StepGear(StepGear... parents) {
      this.parents = Arrays.asList(parents);
      setDependency(BaseTransactionDependency.builder().snapshots(drivers(parents)).writes(driver).build());
    }

    private static List<Driver> drivers(StepGear... parents) {
      List<Driver> result = Lists.newArrayList();
      for (StepGear parent : parents) {
        result.add(parent.driver);
      }
      return result;
    }

    @Override
    public Outcome check(TransactionData transactionData) {
      for (StepGear parent : parents) {
        if (!transactionData.get(parent.driver.reference())) {
          return Outcome.STANDBY;
        }
      }
      return Outcome.SUCCESS;
    }

    @Override
    public Outcome execute(TransactionData transactionData) {
      // no-op
      transactionData.write(driver.reference(), new InMemoryValue<Boolean>(true));
      return Outcome.ABANDON;
    }
  }

  private static class TransferGear extends ConditionalGear implements Gear {

    private final Reference<Integer> src;
    private final Reference<Integer> dst;

    private TransferGear(Driver<Integer> src, Driver<Integer> dst) {
      super(BaseTransactionDependency.builder().writes(src, dst).build());
      this.src = src.reference();
      this.dst = dst.reference();
    }

    @Override
    public Outcome check(TransactionData transactionData) {
      if (transactionData.get(src) > 0 && transactionData.get(dst) == 0) {
        return Outcome.SUCCESS;
      } else {
        return Outcome.STANDBY;
      }
    }

    @Override
    public Outcome execute(TransactionData transactionData) {
      Binding<Integer> source = transactionData.binding(src);
      Binding<Integer> destination = transactionData.binding(dst);
      destination.write(source.read());
      source.write(new InMemoryValue<Integer>(0));
      return Outcome.ABANDON;
    }
  }

  private void run(Gear... gears) throws InterruptedException {
    Worker worker = new NaiveWorker();
    worker.run(gears);
    worker.stop();
    worker.join();
  }

  @Test
  public void testState() throws Exception {

    Value<Integer> v0 = new InMemoryValue<Integer>(0);
    Value<Integer> v1 = new InMemoryValue<Integer>(1);

    final Driver<Integer> driverA = new InMemoryDriver<Integer>(v1);
    final Driver<Integer> driverB = new InMemoryDriver<Integer>(v0);
    final Driver<Integer> driverC = new InMemoryDriver<Integer>(v0);
    final Driver<Integer> driverD = new InMemoryDriver<Integer>(v0);

    Gear gearA = new TransferGear(driverA, driverB);
    Gear gearB = new TransferGear(driverB, driverC);
    Gear gearC = new TransferGear(driverC, driverD);

    run(gearA, gearB, gearC);

    // Check using a transaction
    assertEquals(true, new BaseExecutor().execute(new Function<Boolean>() {
      @Override
      public TransactionDependency dependency() {
        return BaseTransactionDependency.builder().reads(driverA, driverB, driverC, driverD).build();
      }

      @Override
      public Boolean run(TransactionData transactionData) throws Exception {
        return transactionData.get(driverA.reference()) == 0
                   && transactionData.get(driverB.reference()) == 0
                   && transactionData.get(driverC.reference()) == 0
                   && transactionData.get(driverD.reference()) == 1;
      }
    }).result());

    assertEquals(Integer.valueOf(0), driverA.persistence().get());
    assertEquals(Integer.valueOf(0), driverB.persistence().get());
    assertEquals(Integer.valueOf(0), driverC.persistence().get());
    assertEquals(Integer.valueOf(1), driverD.persistence().get());
  }

  @Test
  public void testSteps() throws Exception {
    StepGear stepA = new StepGear();
    StepGear stepB = new StepGear(stepA);
    StepGear stepC = new StepGear(stepA);
    StepGear stepD = new StepGear(stepB, stepC);

    run(stepA, stepB, stepC, stepD);

    assertEquals(true, stepA.driver.persistence().get());
    assertEquals(true, stepB.driver.persistence().get());
    assertEquals(true, stepC.driver.persistence().get());
    assertEquals(true, stepD.driver.persistence().get());
  }
}
