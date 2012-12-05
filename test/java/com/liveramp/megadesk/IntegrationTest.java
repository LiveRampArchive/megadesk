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

import com.liveramp.megadesk.condition.BaseCondition;
import com.liveramp.megadesk.condition.Condition;
import com.liveramp.megadesk.condition.Conditions;
import com.liveramp.megadesk.curator.CuratorMegadesk;
import com.liveramp.megadesk.dependency.Dependencies;
import com.liveramp.megadesk.executor.Executor;
import com.liveramp.megadesk.resource.Resources;
import com.liveramp.megadesk.resource.lib.IntegerResource;
import com.liveramp.megadesk.resource.lib.StringResource;
import com.liveramp.megadesk.step.BaseStep;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.test.BaseTestCase;
import com.netflix.curator.test.TestingServer;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class IntegrationTest extends BaseTestCase {

  public void testWorkflow() throws Exception {

    TestingServer testingServer = new TestingServer(12000);

    final CuratorMegadesk megadesk = new CuratorMegadesk(testingServer.getConnectString());

    final StringResource resourceA = new StringResource(megadesk, "resourceA");
    final StringResource resourceB = new StringResource(megadesk, "resourceB");
    final StringResource resourceC = new StringResource(megadesk, "resourceC");
    final StringResource resourceD = new StringResource(megadesk, "resourceD");
    final IntegerResource resourceE = new IntegerResource(megadesk, "resourceE");
    final IntegerResource resourceF = new IntegerResource(megadesk, "resourceF");

    final Executor executor = new Executor();
    final Semaphore semaphore = new Semaphore(0);

    BaseStep stepZ = new BaseStep(megadesk,
        "stepZ",
        Dependencies.list(resourceA.equals("ready")),
        Resources.list(resourceB, resourceE),
        null) {

      @Override
      public void run() throws Exception {
        write(resourceB, "ready");
        write(resourceE, 0);
      }
    };

    BaseStep stepA = new BaseStep(megadesk,
        "stepA",
        Dependencies.list(resourceA.equals("ready"), resourceB.equals("ready")),
        Resources.list(resourceC),
        null) {

      @Override
      public void run() throws Exception {
        write(resourceC, "done");
      }
    };

    Step stepB = new BaseStep(megadesk,
        "stepB",
        Dependencies.list(resourceC.equals("done"), resourceE.greaterThan(resourceF)),
        Resources.list(resourceD, resourceE, resourceF),
        null) {

      @Override
      public void run() throws Exception {
        Integer eVersion = resourceE.read();
        write(resourceD, "done");
        write(resourceE, eVersion + 1);
        write(resourceF, eVersion);
        if (eVersion < 3) {
          executor.execute(this);
        }
      }
    };

    Condition finished = new BaseCondition(1, TimeUnit.SECONDS) {
      @Override
      public boolean check() throws Exception {
          return resourceF.read() != null
              && resourceF.read() == 3;
      }
    };

    Step stepW = new BaseStep(megadesk,
        "stepW",
        null,
        null,
        Conditions.list(finished)) {

      @Override
      public void run() throws Exception {
        semaphore.release();
      }
    };

    executor.execute(stepA);
    executor.execute(stepB);
    executor.execute(stepZ);
    executor.execute(stepW);

    Thread.sleep(1000);

    resourceA.write("ready");

    semaphore.acquire();

    assertEquals("ready", resourceA.read());
    assertEquals("ready", resourceB.read());
    assertEquals("done", resourceC.read());
    assertEquals("done", resourceD.read());
    assertEquals(Integer.valueOf(4), resourceE.read());
    assertEquals(Integer.valueOf(3), resourceF.read());
  }
}
