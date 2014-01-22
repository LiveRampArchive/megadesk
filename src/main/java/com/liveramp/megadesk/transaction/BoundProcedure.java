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

import java.util.List;

import com.google.common.collect.Lists;

import com.liveramp.megadesk.state.Driver;

public class BoundProcedure implements Procedure {

  private final ProcedureCall call;
  private final Dependency<Driver> dependency;

  public BoundProcedure(ProcedureCall call) {
    this.call = call;
    this.dependency = BaseDependency.<Driver>builder()
                          .snapshots(unbind(call.procedure().dependency().snapshots()))
                          .reads(unbind(call.procedure().dependency().reads()))
                          .writes(unbind(call.procedure().dependency().writes()))
                          .build();
  }

  private List<Driver> unbind(List<String> references) {
    List<Driver> result = Lists.newArrayList();
    for (String reference : references) {
      result.add(call.unbind(reference));
    }
    return result;
  }

  @Override
  public Dependency<Driver> dependency() {
    return dependency;
  }

  @Override
  public void run(Transaction transaction) throws Exception {
    call.procedure().run(new BaseUnboundTransaction(transaction, call));
  }
}
