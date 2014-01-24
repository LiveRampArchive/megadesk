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

package com.liveramp.megadesk.recipes.gear.worker;

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.log4j.Logger;

import com.liveramp.megadesk.core.transaction.Binding;
import com.liveramp.megadesk.recipes.gear.BaseGearExecutor;
import com.liveramp.megadesk.recipes.gear.Gear;
import com.liveramp.megadesk.recipes.gear.GearExecutor;
import com.liveramp.megadesk.recipes.gear.Outcome;

public class NaiveWorker extends BaseWorker implements Worker {

  private static final Logger LOG = Logger.getLogger(NaiveWorker.class);

  private final List<BoundGear> gears;
  private final Thread executor;
  private boolean stopping = false;

  public NaiveWorker() {
    gears = Lists.newArrayList();
    executor = new Thread(new ExecutorRunnable(), this + " executor");
    executor.start();
  }

  @Override
  public void run(Gear gear, Binding binding) {
    synchronized (gears) {
      gears.add(new BoundGear(gear, binding));
    }
  }

  @Override
  public void stop() {
    stopping = true;
  }

  @Override
  public void join() throws InterruptedException {
    executor.join();
  }

  private class ExecutorRunnable implements Runnable {

    private static final long SLEEP_PERIOD_MS = 100;

    private final GearExecutor gearExecutor = new BaseGearExecutor();

    @Override
    public void run() {
      while (true) {
        synchronized (gears) {
          if (stopping && gears.isEmpty()) {
            return;
          }
          Iterator<BoundGear> it = gears.iterator();
          while (it.hasNext()) {
            BoundGear gear = it.next();
            try {
              Outcome outcome = gearExecutor.execute(gear.gear(), gear.binding());
              if (outcome == Outcome.ABANDON) {
                it.remove();
              }
            } catch (Exception e) {
              LOG.error(e); // TODO
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
