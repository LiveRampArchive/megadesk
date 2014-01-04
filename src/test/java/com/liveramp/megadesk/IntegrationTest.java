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
import org.junit.Test;

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.dependency.NodeHierarchyDependency;
import com.liveramp.megadesk.dependency.ReadWriteDependency;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Gears;
import com.liveramp.megadesk.lib.curator.CuratorDriver;
import com.liveramp.megadesk.lib.curator.CuratorGear;
import com.liveramp.megadesk.lib.curator.CuratorNode;
import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.test.BaseTestCase;
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

  private static class TransferGear extends CuratorGear implements Gear {

    private final AtomicInteger resource1;
    private final AtomicInteger resource2;

    public TransferGear(CuratorDriver driver,
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

  public static abstract class StepGear extends CuratorGear implements Gear {

    private boolean isCompleted;
    private List<StepGear> dependencies;

    public StepGear(CuratorDriver driver,
                    String path,
                    StepGear... dependencies) throws Exception {
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
        for (StepGear dependency : dependencies) {
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

  public static class MockStepGear extends StepGear implements Gear {

    public MockStepGear(CuratorDriver driver,
                        String path,
                        StepGear... dependencies) throws Exception {
      super(driver, path, dependencies);
    }

    @Override
    public void doRun() {
      // no op
    }
  }

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

    Gear gearA = new TransferGear(driver, "/a", node1, resource1, node2, resource2);
    Gear gearB = new TransferGear(driver, "/b", node2, resource2, node3, resource3);
    Gear gearC = new TransferGear(driver, "/c", node3, resource3, node4, resource4);

    // Run
    Worker worker = new NaiveWorker();
    worker.run(gearA);
    worker.run(gearB);
    worker.run(gearC);
    worker.join();

    assertEquals(0, resource1.get());
    assertEquals(0, resource2.get());
    assertEquals(0, resource3.get());
    assertEquals(1, resource4.get());
  }

  @Test
  public void testSteps() throws Exception {

    StepGear stepA = new MockStepGear(driver, "/a");
    StepGear stepB = new MockStepGear(driver, "/b", stepA);
    StepGear stepC = new MockStepGear(driver, "/c", stepA);
    StepGear stepD = new MockStepGear(driver, "/d", stepB, stepC);

    // Run
    Worker worker = new NaiveWorker();
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
