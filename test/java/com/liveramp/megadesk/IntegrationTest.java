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

import com.liveramp.megadesk.curator.CuratorMegadesk;
import com.liveramp.megadesk.dependency.Dependencies;
import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.executor.Executor;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.resource.Resources;
import com.liveramp.megadesk.resource.lib.IntegerResource;
import com.liveramp.megadesk.resource.lib.StringResource;
import com.liveramp.megadesk.step.lib.IntegerStep;
import com.liveramp.megadesk.step.lib.SimpleStep;
import com.liveramp.megadesk.test.BaseTestCase;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;
import com.netflix.curator.test.TestingServer;

import java.util.List;
import java.util.concurrent.Semaphore;

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

    final Executor executor = new Executor();
    final Semaphore semaphore = new Semaphore(0);

    SimpleStep stepZ = new SimpleStep(megadesk, "stepZ") {

      @Override
      public List<Dependency> getDependencies() {
        return Dependencies.list(resourceA.equals("ready"));
      }

      @Override
      public List<Resource> getWrites() {
        return Resources.list(resourceB, resourceE);
      }

      @Override
      public void execute() throws Exception {
        write(resourceB, "ready");
        write(resourceE, 0);
      }
    };

    SimpleStep stepA = new SimpleStep(megadesk, "stepA") {

      @Override
      public List<Dependency> getDependencies() {
        return Dependencies.list(resourceA.equals("ready"), resourceB.equals("ready"));
      }

      @Override
      public List<Resource> getWrites() {
        return Resources.list(resourceC);
      }

      @Override
      public void execute() throws Exception {
        write(resourceC, "done");
      }
    };

    IntegerStep stepB = new IntegerStep(megadesk, "stepB") {

      @Override
      public List<Dependency> getDependencies() {
        return Dependencies.list(resourceC.equals("done"), resourceE.greaterThanStep());
      }

      @Override
      public List<Resource> getWrites() {
        return Resources.list(resourceD, resourceE, resourceF);
      }

      @Override
      public void execute() throws Exception {
        Integer eVersion = resourceE.read();
        write(resourceD, "done");
        write(resourceE, eVersion + 1);
        write(resourceF, eVersion);
        set(eVersion);
        if (eVersion < 3) {
          executor.execute(this);
        } else {
          semaphore.release();
        }
      }
    };

    executor.execute(stepA);
    executor.execute(stepB);
    executor.execute(stepZ);

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
