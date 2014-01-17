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
import java.util.concurrent.atomic.AtomicInteger;

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
import com.liveramp.megadesk.lib.curator.CuratorNode;
import com.liveramp.megadesk.lib.curator.CuratorOldGear;
import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.state.BaseGear;
import com.liveramp.megadesk.state.BaseTransaction;
import com.liveramp.megadesk.state.Gear;
import com.liveramp.megadesk.state.NaiveWorker;
import com.liveramp.megadesk.state.Reference;
import com.liveramp.megadesk.state.Transaction;
import com.liveramp.megadesk.state.TransactionData;
import com.liveramp.megadesk.state.Value;
import com.liveramp.megadesk.state.Worker;
import com.liveramp.megadesk.state.lib.InMemoryReference;
import com.liveramp.megadesk.state.lib.InMemoryValue;
import com.liveramp.megadesk.test.BaseTestCase;
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

  private static class TransferOldGear extends CuratorOldGear implements OldGear {

    private final AtomicInteger resource1;
    private final AtomicInteger resource2;

    public TransferOldGear(CuratorDriver driver,
                           String path,
                           Node node1,
                           AtomicInteger resource1,
                           Node node2,
                           AtomicInteger resource2) throws Exception {
      super(driver, path);
      this.resource1 = resource1;
      this.resource2 = resource2;
      depends(new NodeHierarchyDependency(this.getNode()), new TransferDependency(node1, node2));
    }

    private class TransferDependency extends ReadWriteDependency implements Dependency {

      protected TransferDependency(Node node1, Node node2) {
        super(Collections.<Node>emptyList(), Arrays.asList(node1, node2));
      }

      @Override
      public boolean check() {
        return resource1.get() > 0;
      }
    }

    @Override
    public Outcome run() throws Exception {
      resource1.decrementAndGet();
      resource2.incrementAndGet();
      return Outcome.ABANDON;
    }
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

  @Test
  public void testState() throws InterruptedException {

    Value<Integer> v0 = new InMemoryValue<Integer>(0);
    Value<Integer> v1 = new InMemoryValue<Integer>(1);

    Reference<Integer> a = new InMemoryReference<Integer>();
    Reference<Integer> b = new InMemoryReference<Integer>();
    Reference<Integer> c = new InMemoryReference<Integer>();

    Transaction t = new BaseTransaction((List)Arrays.asList(a), (List)Arrays.asList(a));

    t.execution().begin();
    assertEquals(null, t.data().read(a));
    t.data().write(a, v0);
    assertEquals(v0.get(), t.data().read(a).get());
    t.data().write(a, v1);
    assertEquals(v1.get(), t.data().read(a).get());
    t.execution().commit();

    Worker worker = new NaiveWorker();

    Gear gearA = new BaseGear() {
      @Override
      public Outcome run(TransactionData transactionData) throws Exception {
        return Outcome.ABANDON;
      }
    };

    worker.run(gearA);
    worker.stop();
    worker.join();
  }

  @Ignore
  @Test
  public void testMain() throws Exception {

    AtomicInteger resource1 = new AtomicInteger(1);
    AtomicInteger resource2 = new AtomicInteger();
    AtomicInteger resource3 = new AtomicInteger();
    AtomicInteger resource4 = new AtomicInteger();

    Node node1 = new CuratorNode(driver, "/1");
    Node node2 = new CuratorNode(driver, "/2");
    Node node3 = new CuratorNode(driver, "/3");
    Node node4 = new CuratorNode(driver, "/4");

    OldGear gearA = new TransferOldGear(driver, "/a", node1, resource1, node2, resource2);
    OldGear gearB = new TransferOldGear(driver, "/b", node2, resource2, node3, resource3);
    OldGear gearC = new TransferOldGear(driver, "/c", node3, resource3, node4, resource4);

    // Run
    OldWorker worker = new NaiveOldWorker();
    worker.run(gearA);
    worker.run(gearB);
    worker.run(gearC);
    worker.join();

    assertEquals(0, resource1.get());
    assertEquals(0, resource2.get());
    assertEquals(0, resource3.get());
    assertEquals(1, resource4.get());
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
