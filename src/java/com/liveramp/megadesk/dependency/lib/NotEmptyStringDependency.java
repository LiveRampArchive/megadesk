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

package com.liveramp.megadesk.dependency.lib;

import com.liveramp.megadesk.dependency.Dependency;
import com.liveramp.megadesk.dependency.SimpleDependency;

public class NotEmptyStringDependency
    extends SimpleDependency<String>
    implements Dependency<Object, String> {

  @Override
  public boolean check(String resourceData) throws Exception {
    return resourceData != null && resourceData.length() > 0;
  }
}
