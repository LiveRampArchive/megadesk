/**
 *  Copyright 2012 LiveRamp
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

import com.google.common.base.Throwables;
import com.liveramp.megadesk.curator.CuratorMegadesk;
import com.liveramp.megadesk.resource.lib.IntegerResource;
import com.liveramp.megadesk.resource.lib.StringResource;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.step.lib.IntegerStep;
import com.liveramp.megadesk.step.lib.SimpleStep;
import com.liveramp.megadesk.test.BaseTestCase;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.TestingServer;

public class IntegrationTest extends BaseTestCase {

  public void testWorkflow() throws Exception {

    TestingServer testingServer = new TestingServer(12000);
    final CuratorFramework curator;
    curator = CuratorFrameworkFactory.builder()
        .connectionTimeoutMs(1000)
        .retryPolicy(new RetryNTimes(10, 500))
        .connectString(testingServer.getConnectString())
        .build();
    curator.start();

    final CuratorMegadesk megadesk = new CuratorMegadesk(curator);

    final StringResource resourceA = new StringResource(megadesk, "resourceA");
    final StringResource resourceB = new StringResource(megadesk, "resourceB");
    final StringResource resourceC = new StringResource(megadesk, "resourceC");
    final StringResource resourceD = new StringResource(megadesk, "resourceD");
    final IntegerResource resourceE = new IntegerResource(megadesk, "resourceE");
    final IntegerResource resourceF = new IntegerResource(megadesk, "resourceF");

    Thread stepZ = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          SimpleStep step = new SimpleStep(megadesk, "stepZ")
              .reads(resourceA.equals("ready"))
              .writes(resourceB, resourceE);
          step.acquire();
          step.write(resourceB, "ready");
          step.write(resourceE, 0);
          step.release();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, "stepZ");

    Thread stepA = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Step step = new SimpleStep(megadesk, "stepA")
              .reads(resourceA.equals("ready"), resourceB.equals("ready"))
              .writes(resourceC);
          step.acquire();
          step.write(resourceC, "done");
          step.release();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, "stepA");

    Thread stepB = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          IntegerStep step = new IntegerStep(megadesk, "stepB")
              .reads(resourceC.equals("done"), resourceE.greaterThanStep())
              .writes(resourceD, resourceE, resourceF);
          while (true) {
            step.acquire();
            Integer eVersion = resourceE.read();
            step.write(resourceD, "done");
            step.write(resourceE, eVersion + 1);
            step.write(resourceF, eVersion);
            step.set(eVersion);
            step.release();
            if (eVersion == 3) {
              break;
            }
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, "stepB");

    stepA.start();
    stepB.start();
    stepZ.start();

    Thread.sleep(1000);

    resourceA.write("ready");

    stepA.join();
    stepB.join();
    stepZ.join();

    assertEquals("ready", resourceA.read());
    assertEquals("ready", resourceB.read());
    assertEquals("done", resourceC.read());
    assertEquals("done", resourceD.read());
    assertEquals(Integer.valueOf(4), resourceE.read());
    assertEquals(Integer.valueOf(3), resourceF.read());
  }
}
