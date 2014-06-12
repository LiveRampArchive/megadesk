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

package com.liveramp.megadesk.recipes.transaction;

import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseTransaction;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;

public class Copy<VALUE> extends BaseTransaction<VALUE> {

  private final Variable<VALUE> source;
  private final Variable<VALUE> destination;

  public Copy(Variable<VALUE> source, Variable<VALUE> destination) {
    super(BaseDependency.builder().reads(source).writes(destination).build());
    this.source = source;
    this.destination = destination;
  }

  @Override
  public VALUE run(Context context) throws Exception {
    VALUE value = context.read(source);
    context.write(destination, value);
    return value;
  }
}
