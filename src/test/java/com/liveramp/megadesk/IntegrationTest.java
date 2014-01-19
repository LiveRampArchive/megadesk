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
import java.util.Collections;
import java.util.List;

import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.liveramp.megadesk.gear.ConditionalGear;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Outcome;
import com.liveramp.megadesk.old.dependency.Dependency;
import com.liveramp.megadesk.old.dependency.lib.NodeHierarchyDependency;
import com.liveramp.megadesk.old.dependency.lib.ReadWriteDependency;
import com.liveramp.megadesk.old.gear.Gears;
import com.liveramp.megadesk.old.gear.OldGear;
import com.liveramp.megadesk.old.lib.curator.CuratorDriver;
import com.liveramp.megadesk.old.lib.curator.CuratorOldGear;
import com.liveramp.megadesk.old.node.Node;
import com.liveramp.megadesk.old.worker.NaiveOldWorker;
import com.liveramp.megadesk.old.worker.OldWorker;
import com.liveramp.megadesk.state.Driver;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.state.lib.InMemoryDriver;
import com.liveramp.megadesk.state.lib.InMemoryValue;
import com.liveramp.megadesk.test.BaseTestCase;
import com.liveramp.megadesk.transaction.BaseTransactionDependency;
import com.liveramp.megadesk.transaction.TransactionData;
import com.liveramp.megadesk.worker.NaiveWorker;
import com.liveramp.megadesk.worker.Worker;

import static org.junit.Assert.assertEquals;

public class IntegrationTest extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

  private TestingServer testingServer;
  private CuratorDriver driver;

  @Before
  public void setUpDriver() throws Exception {
    this.testingServer = new TestingServer(12000);
    this.driver = new CuratorDriver(testingServer.getConnectString());
  }

  @After
  public void tearDownDriver() throws IOException {
    this.testingServer.close();
  }

  public static abstract class StepOldGear extends CuratorOldGear implements OldGear {

    private boolean isCompleted;
    private List<StepOldGear> dependencies;

    public StepOldGear(CuratorDriver driver,
                       String path,
                       StepOldGear... dependencies) throws Exception {
      super(driver, path);
      this.isCompleted = false;
      this.dependencies = Arrays.asList(dependencies);
      depends(new NodeHierarchyDependency(this.getNode()), new StepGearDependency());
    }

    private class StepGearDependency extends ReadWriteDependency implements Dependency {

      protected StepGearDependency() {
        super(Gears.getNodes(dependencies), Collections.<Node>emptyList());
      }

      @Override
      public boolean check() {
        for (StepOldGear dependency : dependencies) {
          if (!dependency.isCompleted()) {
            return false;
          }
        }
        return true;
      }
    }

    public abstract void doRun();

    @Override
    public Outcome run() throws Exception {
      doRun();
      setCompleted(true);
      return Outcome.ABANDON;
    }

    public boolean isCompleted() {
      return isCompleted;
    }

    private void setCompleted(boolean isCompleted) {
      this.isCompleted = isCompleted;
    }
  }

  public static class MockStepOldGear extends StepOldGear implements OldGear {

    public MockStepOldGear(CuratorDriver driver,
                           String path,
                           StepOldGear... dependencies) throws Exception {
      super(driver, path, dependencies);
    }

    @Override
    public void doRun() {
      // no op
    }
  }

  private static class TransferGear extends ConditionalGear implements Gear {

    private final Reference<Integer> src;
    private final Reference<Integer> dst;

    private TransferGear(Driver<Integer> src, Driver<Integer> dst) {
      super(BaseTransactionDependency.builder().reads(src).writes(src, dst).build());
      this.src = src.reference();
      this.dst = dst.reference();
    }

    @Override
    public Outcome check(TransactionData transactionData) {
      if (transactionData.get(src) > 0) {
        return Outcome.SUCCESS;
      } else {
        return Outcome.STANDBY;
      }
    }

    @Override
    public Outcome execute(TransactionData transactionData) {
      transactionData.write(dst, transactionData.read(src));
      transactionData.write(src, new InMemoryValue<Integer>(0));
      return Outcome.ABANDON;
    }
  }

  @Test
  public void testState() throws InterruptedException {

    Value<Integer> v0 = new InMemoryValue<Integer>(0);
    Value<Integer> v1 = new InMemoryValue<Integer>(1);

    Driver<Integer> driverA = new InMemoryDriver<Integer>(v1);
    Driver<Integer> driverB = new InMemoryDriver<Integer>(v0);
    Driver<Integer> driverC = new InMemoryDriver<Integer>(v0);
    Driver<Integer> driverD = new InMemoryDriver<Integer>(v0);

    Worker worker = new NaiveWorker();

    Gear gearA = new TransferGear(driverA, driverB);
    Gear gearB = new TransferGear(driverB, driverC);
    Gear gearC = new TransferGear(driverC, driverD);

    worker.run(gearA);
    worker.run(gearB);
    worker.run(gearC);
    worker.stop();
    worker.join();

    assertEquals(Integer.valueOf(0), driverA.persistence().read().get());
    assertEquals(Integer.valueOf(0), driverB.persistence().read().get());
    assertEquals(Integer.valueOf(0), driverC.persistence().read().get());
    assertEquals(Integer.valueOf(1), driverD.persistence().read().get());
  }

  @Ignore
  @Test
  public void testSteps() throws Exception {

    StepOldGear stepA = new MockStepOldGear(driver, "/a");
    StepOldGear stepB = new MockStepOldGear(driver, "/b", stepA);
    StepOldGear stepC = new MockStepOldGear(driver, "/c", stepA);
    StepOldGear stepD = new MockStepOldGear(driver, "/d", stepB, stepC);

    // Run
    OldWorker worker = new NaiveOldWorker();
    worker.run(stepA);
    worker.run(stepB);
    worker.run(stepC);
    worker.run(stepD);
    worker.join();

    assertEquals(true, stepA.isCompleted);
    assertEquals(true, stepB.isCompleted);
    assertEquals(true, stepC.isCompleted);
    assertEquals(true, stepD.isCompleted);
  }
}
