/**
 *  Copyright 2012 LiveRamp
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

package com.liveramp.megadesk.step.lib;

import com.liveramp.megadesk.Megadesk;
import com.liveramp.megadesk.serialization.lib.ThriftJsonSerialization;
import com.liveramp.megadesk.step.BaseStep;
import com.liveramp.megadesk.step.Step;
import org.apache.thrift.TBase;

public class ThriftStep<T extends TBase>
    extends BaseStep<T, ThriftStep<T>>
    implements Step<T, ThriftStep<T>> {

  public ThriftStep(String id,
                    Megadesk megadesk,
                    T baseObject) throws Exception {
    super(id, megadesk, new ThriftJsonSerialization<T>(baseObject));
  }
}
