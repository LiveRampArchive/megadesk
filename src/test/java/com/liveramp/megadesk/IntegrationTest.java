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

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.dependency.lib.NodeHierarchyDependency;
import com.liveramp.megadesk.dependency.lib.ReadWriteDependency;
import com.liveramp.megadesk.gear.Gears;
import com.liveramp.megadesk.gear.OldGear;
import com.liveramp.megadesk.lib.curator.CuratorDriver;
import com.liveramp.megadesk.lib.curator.CuratorOldGear;
import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.state.BaseTransactionDependency;
import com.liveramp.megadesk.state.ConditionalGear;
import com.liveramp.megadesk.state.Gear;
import com.liveramp.megadesk.state.NaiveWorker;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.TransactionData;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.state.Worker;
import com.liveramp.megadesk.state.lib.InMemoryReference;
import com.liveramp.megadesk.state.lib.InMemoryValue;
import com.liveramp.megadesk.test.BaseTestCase;
import com.liveramp.megadesk.utils.FormatUtils;
import com.liveramp.megadesk.worker.NaiveOldWorker;
import com.liveramp.megadesk.worker.OldWorker;

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

    private TransferGear(Reference<Integer> src, Reference<Integer> dst) {
      super(BaseTransactionDependency.builder().reads(src).writes(src, dst).build());
      this.src = src;
      this.dst = dst;
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
      LOG.info("Executing " + FormatUtils.formatToString(this, ""));
      transactionData.write(dst, transactionData.read(src));
      transactionData.write(src, new InMemoryValue<Integer>(0));
      return Outcome.ABANDON;
    }
  }

  @Test
  public void testTransaction() {
    //    Transaction t = new BaseTransaction((List)Arrays.asList(a), (List)Arrays.asList(a));
    //
    //    t.execution().begin();
    //    assertEquals(null, t.data().read(a));
    //    t.data().write(a, v0);
    //    assertEquals(v0.get(), t.data().read(a).get());
    //    t.data().write(a, v1);
    //    assertEquals(v1.get(), t.data().read(a).get());
    //    t.execution().commit();
  }

  @Test
  public void testState() throws InterruptedException {

    Value<Integer> v0 = new InMemoryValue<Integer>(0);
    Value<Integer> v1 = new InMemoryValue<Integer>(1);

    Reference<Integer> a = new InMemoryReference<Integer>(v1);
    Reference<Integer> b = new InMemoryReference<Integer>(v0);
    Reference<Integer> c = new InMemoryReference<Integer>(v0);
    Reference<Integer> d = new InMemoryReference<Integer>(v0);

    Worker worker = new NaiveWorker();

    Gear gearA = new TransferGear(a, b);
    Gear gearB = new TransferGear(b, c);
    Gear gearC = new TransferGear(c, d);

    worker.run(gearA);
    worker.run(gearB);
    worker.run(gearC);
    worker.stop();
    worker.join();

    assertEquals(Integer.valueOf(0), a.read().get());
    assertEquals(Integer.valueOf(0), b.read().get());
    assertEquals(Integer.valueOf(0), c.read().get());
    assertEquals(Integer.valueOf(1), d.read().get());
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
