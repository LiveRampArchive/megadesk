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
import com.liveramp.megadesk.curator.IntegerResource;
import com.liveramp.megadesk.curator.IntegerManeuver;
import com.liveramp.megadesk.curator.SimpleManeuver;
import com.liveramp.megadesk.curator.StringResource;
import com.liveramp.megadesk.maneuver.Maneuver;
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

    final StringResource resourceA = new StringResource(curator, "resourceA");
    final StringResource resourceB = new StringResource(curator, "resourceB");
    final StringResource resourceC = new StringResource(curator, "resourceC");
    final StringResource resourceD = new StringResource(curator, "resourceD");
    final IntegerResource resourceE = new IntegerResource(curator, "resourceE");
    final IntegerResource resourceF = new IntegerResource(curator, "resourceF");

    Thread maneuverZ = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          SimpleManeuver maneuver = new SimpleManeuver(curator, "maneuverZ")
              .reads(resourceA.at("ready"))
              .writes(resourceB, resourceE);
          maneuver.acquire();
          maneuver.write(resourceB, "ready");
          maneuver.write(resourceE, 0);
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
          Maneuver maneuver = new SimpleManeuver(curator, "maneuverA")
              .reads(resourceA.at("ready"), resourceB.at("ready"))
              .writes(resourceC);
          maneuver.acquire();
          maneuver.write(resourceC, "done");
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
          IntegerManeuver maneuver = new IntegerManeuver(curator, "maneuverB")
              .writes(resourceD, resourceE, resourceF);
          Integer processedVersion = -1;
          while (processedVersion < 2) {
            processedVersion = maneuver.getData();
            if (processedVersion == null) {
              processedVersion = -1;
            }
            maneuver.reads(resourceC.at("done"), resourceE.greaterThan(processedVersion));
            maneuver.acquire();
            Integer eVersion = resourceE.getData();
            maneuver.write(resourceD, "done");
            maneuver.write(resourceE, eVersion + 1);
            maneuver.write(resourceF, eVersion);
            maneuver.setData(eVersion);
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

    resourceA.setData("ready");

    maneuverA.join();
    maneuverB.join();
    maneuverZ.join();

    assertEquals("ready", resourceA.getData());
    assertEquals("ready", resourceB.getData());
    assertEquals("done", resourceC.getData());
    assertEquals("done", resourceD.getData());
    assertEquals(Integer.valueOf(4), resourceE.getData());
    assertEquals(Integer.valueOf(3), resourceF.getData());
  }
}
