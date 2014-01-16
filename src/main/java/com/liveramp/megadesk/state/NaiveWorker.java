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

package com.liveramp.megadesk.state;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.liveramp.megadesk.attempt.Outcome;

public class NaiveWorker implements Worker {

  private final List<Gear> gears;
  private final Thread executor;
  private boolean stopping = false;

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

  @Override
  public void stop() {
    stopping = true;
  }

  //  private void stop(OldGear gear) {
  //    synchronized (gears) {
  //      gears.remove(gear);
  //    }
  //  }

  @Override
  public void join() throws InterruptedException {
    executor.join();
  }

  private class ExecutorRunnable implements Runnable {

    private static final long SLEEP_PERIOD_MS = 100;

    private final GearExecutor gearExecutor = new GearExecutor();

    @Override
    public void run() {
      while (true) {
        synchronized (gears) {
          if (stopping && gears.isEmpty()) {
            return;
          }
          Iterator<Gear> it = gears.iterator();
          while (it.hasNext()) {
            Gear gear = it.next();
            try {
              Outcome outcome = gearExecutor.execute(gear, new BaseTransaction().depends(gear.dependency()));
              if (outcome == Outcome.ABANDON) {
                it.remove();
              }
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
