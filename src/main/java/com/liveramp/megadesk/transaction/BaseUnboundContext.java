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

package com.liveramp.megadesk.transaction;

import com.liveramp.megadesk.state.Reference;

public class BaseUnboundContext implements UnboundContext {

  private final Context context;
  private final TransactionBinding call;

  public BaseUnboundContext(Context context, TransactionBinding call) {
    this.context = context;
    this.call = call;
  }

  @Override
  public <VALUE> Binding<VALUE> binding(String reference) {
    return context.binding((Reference<VALUE>)call.unbind(reference).reference());
  }

  @Override
  public <VALUE> VALUE read(String reference) {
    return context.read((Reference<VALUE>)call.unbind(reference).reference());
  }

  @Override
  public <VALUE> void write(String reference, VALUE value) {
    context.write(call.unbind(reference).reference(), value);
  }
}
