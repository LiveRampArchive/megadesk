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

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import com.liveramp.megadesk.core.state.Driver;
import com.liveramp.megadesk.core.transaction.UnboundTransaction;

public class TransactionBinding<V> {

  private final UnboundTransaction<V> transaction;
  private final List<Driver> arguments;
  private final Map<String, Driver> mapping;

  public TransactionBinding(UnboundTransaction<V> transaction, List<Driver> arguments) {
    this.transaction = transaction;
    this.arguments = ImmutableList.copyOf(arguments);
    this.mapping = Maps.newHashMap();
    for (int i = 0; i < arguments.size(); ++i) {
      mapping.put(transaction.arguments().get().get(i), arguments.get(i));
    }
  }

  public Driver unbind(String reference) {
    if (!mapping.containsKey(reference)) {
      throw new IllegalStateException(); // TODO message
    }
    return mapping.get(reference);
  }

  public UnboundTransaction<V> transaction() {
    return transaction;
  }

  public List<Driver> arguments() {
    return arguments;
  }
}
