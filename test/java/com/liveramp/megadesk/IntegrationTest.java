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
import com.liveramp.megadesk.curator.IntegerDevice;
import com.liveramp.megadesk.curator.IntegerManeuver;
import com.liveramp.megadesk.curator.SimpleManeuver;
import com.liveramp.megadesk.curator.StringDevice;
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

    final StringDevice deviceA = new StringDevice(curator, "deviceA");
    final StringDevice deviceB = new StringDevice(curator, "deviceB");
    final StringDevice deviceC = new StringDevice(curator, "deviceC");
    final StringDevice deviceD = new StringDevice(curator, "deviceD");
    final IntegerDevice deviceE = new IntegerDevice(curator, "deviceE");
    final IntegerDevice deviceF = new IntegerDevice(curator, "deviceF");

    Thread maneuverZ = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          SimpleManeuver maneuver = new SimpleManeuver(curator, "maneuverZ")
              .reads(deviceA.at("ready"))
              .writes(deviceB, deviceE);
          maneuver.acquire();
          maneuver.write(deviceB, "ready");
          maneuver.write(deviceE, 0);
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
              .reads(deviceA.at("ready"), deviceB.at("ready"))
              .writes(deviceC);
          maneuver.acquire();
          maneuver.write(deviceC, "done");
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
          IntegerManeuver maneuver = new IntegerManeuver(curator, "maneuverB")
              .reads(deviceC.at("done"), deviceE.greaterThan(processedEVersion))
              .writes(deviceD, deviceE, deviceF);
          while (processedEVersion < 2) {
            maneuver.acquire();
            maneuver.setStatus(-1);
            processedEVersion = deviceE.getStatus();
            maneuver.write(deviceD, "done");
            maneuver.write(deviceE, processedEVersion + 1);
            maneuver.write(deviceF, processedEVersion);
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

    deviceA.setStatus("ready");

    maneuverA.join();
    maneuverB.join();
    maneuverZ.join();

    assertEquals("ready", deviceA.getStatus());
    assertEquals("ready", deviceB.getStatus());
    assertEquals("done", deviceC.getStatus());
    assertEquals("done", deviceD.getStatus());
    assertEquals(Integer.valueOf(3), deviceE.getStatus());
    assertEquals(Integer.valueOf(2), deviceF.getStatus());
  }
}
