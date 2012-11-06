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

package com.liveramp.megadesk.data.lib;

import com.liveramp.megadesk.data.DataCheck;
import com.liveramp.megadesk.resource.Resource;

public class NotEmptyStringDataCheck implements DataCheck<String> {

  @Override
  public boolean check(Resource<String> resource) throws Exception {
    String data = resource.read();
    return data != null && data.length() > 0;
  }
}
