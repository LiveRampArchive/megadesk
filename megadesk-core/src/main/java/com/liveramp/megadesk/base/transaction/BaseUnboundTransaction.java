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

package com.liveramp.megadesk.base.transaction;

import com.liveramp.megadesk.core.transaction.Dependency;
import com.liveramp.megadesk.core.transaction.UnboundTransaction;

public abstract class BaseUnboundTransaction<V> implements UnboundTransaction<V> {

  private final Arguments arguments;
  private final BaseDependency<String> dependency;

  public BaseUnboundTransaction(Arguments arguments, BaseDependency<String> dependency) {
    this.arguments = arguments;
    this.dependency = dependency;
    // TODO check consistency
  }

  @Override
  public Arguments arguments() {
    return arguments;
  }

  @Override
  public Dependency<String> dependency() {
    return dependency;
  }
}
