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

package com.liveramp.megadesk.refactor.node;

public final class Paths {

  private Paths() {
  }

  public static String append(String... parts) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < parts.length; ++i) {
      if (i != 0) {
        builder.append("/");
      }
      builder.append(parts[i]);
    }
    return builder.toString();
  }

}
