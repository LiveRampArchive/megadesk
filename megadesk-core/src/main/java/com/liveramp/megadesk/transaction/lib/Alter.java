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

package com.liveramp.megadesk.transaction.lib;

import com.liveramp.megadesk.transaction.Arguments;
import com.liveramp.megadesk.transaction.BaseDependency;
import com.liveramp.megadesk.transaction.BaseUnboundTransaction;
import com.liveramp.megadesk.transaction.UnboundContext;
import com.liveramp.megadesk.transaction.UnboundTransaction;

public abstract class Alter<V> extends BaseUnboundTransaction<V> implements UnboundTransaction<V> {

  public Alter() {
    super(new Arguments("input"),
             BaseDependency.<String>builder().writes("input").build());
  }

  @Override
  public V run(UnboundContext transaction) throws Exception {
    V result = alter(transaction.<V>read("input"));
    transaction.write("input", result);
    return result;
  }

  public abstract V alter(V value);
}
