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

package com.liveramp.megadesk.gear;

import com.liveramp.megadesk.old.attempt.Outcome;
import com.liveramp.megadesk.transaction.BaseTransactionDependency;
import com.liveramp.megadesk.transaction.TransactionData;

public abstract class ConditionalGear extends BaseGear implements Gear {

  public ConditionalGear(BaseTransactionDependency dependency) {
    super(dependency);
  }

  public abstract Outcome check(TransactionData transactionData);

  public abstract Outcome execute(TransactionData transactionData);

  @Override
  public final Outcome run(TransactionData transactionData) throws Exception {
    Outcome check = check(transactionData);
    if (check == Outcome.SUCCESS) {
      return execute(transactionData);
    } else {
      return check;
    }
  }
}
