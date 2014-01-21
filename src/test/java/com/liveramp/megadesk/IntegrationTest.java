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
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.BaseExecutor;
import com.liveramp.megadesk.transaction.Binding;
import com.liveramp.megadesk.transaction.Dependency;
import com.liveramp.megadesk.transaction.Function;
import com.liveramp.megadesk.transaction.Procedure;
import com.liveramp.megadesk.transaction.Transaction;
import com.liveramp.megadesk.transaction.lib.Alter;
import com.liveramp.megadesk.worker.NaiveWorker;

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
      setDependency(BaseDependency.builder().snapshots(drivers(parents)).writes(driver).build());
    }

    private static List<Driver> drivers(StepGear... parents) {
      List<Driver> result = Lists.newArrayList();
      for (StepGear parent : parents) {
        result.add(parent.driver);
      }
      return result;
    }

    @Override
    public Outcome check(Transaction transaction) {
      for (StepGear parent : parents) {
        if (!transaction.get(parent.driver.reference())) {
          return Outcome.STANDBY;
        }
      }
      return Outcome.SUCCESS;
    }

    @Override
    public Outcome execute(Transaction transaction) {
      // no-op
      transaction.write(driver.reference(), new InMemoryValue<Boolean>(true));
      return Outcome.ABANDON;
    }
  }

  private static class TransferGear extends ConditionalGear implements Gear {

    private final Reference<Integer> src;
    private final Reference<Integer> dst;

    private TransferGear(Driver<Integer> src, Driver<Integer> dst) {
      super(BaseDependency.builder().writes(src, dst).build());
      this.src = src.reference();
      this.dst = dst.reference();
    }

    @Override
    public Outcome check(Transaction transaction) {
      if (transaction.get(src) > 0 && transaction.get(dst) == 0) {
        return Outcome.SUCCESS;
      } else {
        return Outcome.STANDBY;
      }
    }

    @Override
    public Outcome execute(Transaction transaction) {
      Binding<Integer> source = transaction.binding(src);
      Binding<Integer> destination = transaction.binding(dst);
      destination.write(source.read());
      source.write(new InMemoryValue<Integer>(0));
      return Outcome.ABANDON;
    }
  }

  private void execute(Gear... gears) throws InterruptedException {
    new NaiveWorker().complete(gears);
  }

  private void execute(Procedure procedure) throws Exception {
    new BaseExecutor().execute(procedure);
  }

  private <V> Value<V> execute(Function<V> function) throws Exception {
    return new BaseExecutor().execute(function);
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

    execute(gearA, gearB, gearC);

    // Check using a transaction
    assertEquals(true, execute(new Function<Boolean>() {

      @Override
      public Dependency dependency() {
        return BaseDependency.builder().reads(driverA, driverB, driverC, driverD).build();
      }

      @Override
      public Value<Boolean> call(Transaction transaction) throws Exception {
        return new InMemoryValue<Boolean>(transaction.get(driverA.reference()) == 0
                                              && transaction.get(driverB.reference()) == 0
                                              && transaction.get(driverC.reference()) == 0
                                              && transaction.get(driverD.reference()) == 1);
      }
    }).get());

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

    execute(stepA, stepB, stepC, stepD);

    assertEquals(true, stepA.driver.persistence().get());
    assertEquals(true, stepB.driver.persistence().get());
    assertEquals(true, stepC.driver.persistence().get());
    assertEquals(true, stepD.driver.persistence().get());
  }

  @Test
  public void testBatch() throws Exception {
    final Value<Integer> v0 = new InMemoryValue<Integer>(0);
    final Driver<Integer> driverA = new InMemoryDriver<Integer>(v0);
    final Driver<Integer> driverB = new InMemoryDriver<Integer>(v0);

    Alter<Integer> incrementA = new Alter<Integer>(driverA) {
      @Override
      public Value<Integer> alter(Value<Integer> value) {
        return new InMemoryValue<Integer>(value.get() + 1);
      }
    };

    // Increment A twice
    execute(incrementA);
    execute(incrementA);
    // Transfer A to B
    execute(new TransferGear(driverA, driverB));

    assertEquals(Integer.valueOf(0), driverA.persistence().get());
    assertEquals(Integer.valueOf(2), driverB.persistence().get());
  }
}
