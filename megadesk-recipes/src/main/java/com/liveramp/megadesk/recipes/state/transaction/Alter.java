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

package com.liveramp.megadesk.recipes.state.transaction;

import com.liveramp.megadesk.base.state.Arg;
import com.liveramp.megadesk.base.transaction.BaseDependency;
import com.liveramp.megadesk.base.transaction.BaseTransaction;
import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.Context;
import com.liveramp.megadesk.core.transaction.Transaction;

public abstract class Alter<VALUE> extends BaseTransaction<VALUE> implements Transaction<VALUE> {

  private final Variable<VALUE> variable;

  public Alter() {
    this(new Arg<VALUE>(0));
  }

  public Alter(Variable<VALUE> variable) {
    super(BaseDependency.builder().writes(variable).build());
    this.variable = variable;
  }

  @Override
  public VALUE run(Context context) throws Exception {
    VALUE value = alter(context.read(variable));
    context.write(variable, value);
    return value;
  }

  protected abstract VALUE alter(VALUE read);
}
