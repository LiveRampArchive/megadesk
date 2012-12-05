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

package com.liveramp.megadesk.condition;

public abstract class BaseCondition implements Condition {

  private final long timeoutMs;

  public BaseCondition(long timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  @Override
  public boolean check(final ConditionWatcher watcher) {
    new TimeoutWatcher(timeoutMs) {
      @Override
      public void onTimeout() {
        watcher.onChange();
      }
    };
    return check();
  }

  public abstract boolean check();
}
