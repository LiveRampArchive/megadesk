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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.test.TestingServer;
import org.apache.log4j.Logger;

import com.liveramp.megadesk.attempt.Outcome;
import com.liveramp.megadesk.gear.Gear;
import com.liveramp.megadesk.gear.Gears;
import com.liveramp.megadesk.lib.curator.CuratorDriver;
import com.liveramp.megadesk.lib.curator.CuratorGear;
import com.liveramp.megadesk.lib.curator.CuratorNode;
import com.liveramp.megadesk.node.Node;
import com.liveramp.megadesk.worker.NaiveWorker;
import com.liveramp.megadesk.worker.Worker;
import com.liveramp.megadesk.test.BaseTestCase;

public class IntegrationTest extends BaseTestCase {

  private static final Logger LOG = Logger.getLogger(IntegrationTest.class);

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
      writes(node1, node2);
    }

    @Override
    public boolean isRunnable() {
      return resource1.get() > 0;
    }

    @Override
    public Outcome run() throws Exception {
      resource1.decrementAndGet();
      resource2.incrementAndGet();
      return Outcome.SUCCESS;
    }
  }

  public static abstract class StepGear extends CuratorGear implements Gear {

    public StepGear(CuratorDriver driver,
                    String path,
                    Gear... dependencies) throws Exception {
      super(driver, path);
      reads(Gears.getNodes(dependencies));
    }

    public abstract void doRun();

    @Override
    public boolean isRunnable() {
      return false;  // TODO
    }

    @Override
    public Outcome run() throws Exception {
      doRun();
      return Outcome.END;
    }
  }

  public static class TransferStepGear extends StepGear implements Gear {

    private final AtomicBoolean resource;

    public TransferStepGear(CuratorDriver driver,
                            String path,
                            AtomicBoolean resource,
                            Gear... dependencies) throws Exception {
      super(driver, path, dependencies);
      this.resource = resource;
    }

    @Override
    public void doRun() {
      resource.set(true);
    }
  }

  public void testMain() throws Exception {
    TestingServer testingServer = new TestingServer(12000);
    CuratorDriver driver = new CuratorDriver(testingServer.getConnectString());

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
  }

  public void testSteps() throws Exception {
    TestingServer testingServer = new TestingServer(12000);
    CuratorDriver driver = new CuratorDriver(testingServer.getConnectString());

    AtomicBoolean resourceA = new AtomicBoolean(false);
    AtomicBoolean resourceB = new AtomicBoolean(false);
    AtomicBoolean resourceC = new AtomicBoolean(false);
    AtomicBoolean resourceD = new AtomicBoolean(false);

    Gear stepA = new TransferStepGear(driver, "/a", resourceA);
    Gear stepB = new TransferStepGear(driver, "/b", resourceB, stepA);
    Gear stepC = new TransferStepGear(driver, "/c", resourceC, stepA);
    Gear stepD = new TransferStepGear(driver, "/d", resourceD, stepB, stepC);

    // Run
    Worker worker = new NaiveWorker();
    worker.run(stepA);
    worker.run(stepB);
    worker.run(stepC);
    worker.run(stepD);
    worker.join();
  }

}
