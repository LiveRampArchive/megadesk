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

package com.liveramp.megadesk.refactor.worker;

import java.util.ArrayList;
import java.util.List;

import com.liveramp.megadesk.refactor.gear.Gear;

public class NaiveWorker implements Worker {

  private final List<Gear> gears;
  private final Thread executor;

  public NaiveWorker() {
    gears = new ArrayList<Gear>();
    executor = new Thread(new ExecutorRunnable(), this + " executor");
    executor.start();
  }

  @Override
  public void run(Gear gear) {
    synchronized (gears) {
      gears.add(gear);
    }
  }

  private class ExecutorRunnable implements Runnable {

    private static final long SLEEP_PERIOD_MS = 1000;

    private final GearExecutor gearExecutor = new GearExecutor();

    @Override
    public void run() {
      while (true) {
        synchronized (gears) {
          for (Gear gear : gears) {
            try {
              gearExecutor.execute(gear);
            } catch (Exception e) {
              throw new RuntimeException(e); // TODO
            }
          }
        }
        try {
          Thread.sleep(SLEEP_PERIOD_MS);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
}
