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

package com.liveramp.megadesk.state;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BaseTransactionDependency implements TransactionDependency {

  private List<Reference> reads;
  private List<Reference> writes;

  public BaseTransactionDependency() {
    this.reads = Collections.emptyList();
    this.writes = Collections.emptyList();
  }

  public BaseTransactionDependency(List<Reference> reads,
                                   List<Reference> writes) {
    this.reads = Collections.unmodifiableList(reads);
    this.writes = Collections.unmodifiableList(writes);
  }

  @Override
  public List<Reference> reads() {
    return reads;
  }

  @Override
  public List<Reference> writes() {
    return writes;
  }

  public BaseTransactionDependency reads(Reference... references) {
    return reads(Arrays.asList(references));
  }

  public BaseTransactionDependency reads(List<Reference> references) {
    this.reads = Collections.unmodifiableList(references);
    return this;
  }

  public BaseTransactionDependency writes(Reference... references) {
    return writes(Arrays.asList(references));
  }

  public BaseTransactionDependency writes(List<Reference> references) {
    this.writes = Collections.unmodifiableList(references);
    return this;
  }
}
