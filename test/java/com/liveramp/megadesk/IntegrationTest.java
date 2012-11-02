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
import com.liveramp.megadesk.curator.CuratorManeuver;
import com.liveramp.megadesk.curator.StringCuratorResource;
import com.liveramp.megadesk.curator.VersionedCuratorResource;
import com.liveramp.megadesk.maneuver.Maneuver;
import com.liveramp.megadesk.resource.Reads;
import com.liveramp.megadesk.resource.Writes;
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

    final StringCuratorResource resourceA = new StringCuratorResource(curator, "resourceA");
    final StringCuratorResource resourceB = new StringCuratorResource(curator, "resourceB");
    final StringCuratorResource resourceC = new StringCuratorResource(curator, "resourceC");
    final StringCuratorResource resourceD = new StringCuratorResource(curator, "resourceD");
    final VersionedCuratorResource resourceE = new VersionedCuratorResource(curator, "resourceE");
    final VersionedCuratorResource resourceF = new VersionedCuratorResource(curator, "resourceF");

    Thread maneuverZ = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Maneuver maneuver = new CuratorManeuver(curator,
              "maneuverZ",
              Reads.list(resourceA.at("ready")),
              Writes.list(resourceB, resourceE));
          maneuver.acquire();
          maneuver.set(resourceB, "ready");
          maneuver.set(resourceE, 0);
          maneuver.release();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, "maneuverZ");

    Thread maneuverA = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Maneuver maneuver = new CuratorManeuver(curator,
              "maneuverA",
              Reads.list(resourceA.at("ready"), resourceB.at("ready")),
              Writes.list(resourceC));
          maneuver.acquire();
          maneuver.set(resourceC, "done");
          maneuver.release();
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, "maneuverA");

    Thread maneuverB = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          int processedEVersion = -1;
          while (processedEVersion < 2) {
            Maneuver maneuver = new CuratorManeuver(curator,
                "maneuverB",
                Reads.list(resourceC.at("done"), resourceE.greaterThan(processedEVersion)),
                Writes.list(resourceD, resourceE, resourceF));
            maneuver.acquire();
            processedEVersion = resourceE.getState();
            maneuver.set(resourceD, "done");
            maneuver.set(resourceE, processedEVersion + 1);
            maneuver.set(resourceF, processedEVersion);
            maneuver.release();
          }
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }, "maneuverB");

    maneuverA.start();
    maneuverB.start();
    maneuverZ.start();

    Thread.sleep(1000);

    resourceA.setState("ready");

    maneuverA.join();
    maneuverB.join();
    maneuverZ.join();

    assertEquals("ready", resourceA.getState());
    assertEquals("ready", resourceB.getState());
    assertEquals("done", resourceC.getState());
    assertEquals("done", resourceD.getState());
    assertEquals(Integer.valueOf(3), resourceE.getState());
    assertEquals(Integer.valueOf(2), resourceF.getState());
  }
}
