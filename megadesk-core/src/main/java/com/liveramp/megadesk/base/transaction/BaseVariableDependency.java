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

import org.apache.commons.lang.builder.CompareToBuilder;

import com.liveramp.megadesk.core.state.Variable;
import com.liveramp.megadesk.core.transaction.DependencyType;
import com.liveramp.megadesk.core.transaction.VariableDependency;

public class BaseVariableDependency<VALUE> implements VariableDependency<VALUE> {

  private final Variable<VALUE> variable;
  private final DependencyType type;

  public BaseVariableDependency(Variable<VALUE> variable, DependencyType type) {
    this.variable = variable;
    this.type = type;
  }

  @Override
  public Variable<VALUE> variable() {
    return variable;
  }

  @Override
  public DependencyType type() {
    return type;
  }

  public static <VALUE> BaseVariableDependency<VALUE> build(Variable<VALUE> variable, DependencyType type) {
    return new BaseVariableDependency<VALUE>(variable, type);
  }

  @Override
  public int compareTo(VariableDependency<VALUE> o) {
    return new CompareToBuilder().append(variable, o.variable()).toComparison();
  }
}
