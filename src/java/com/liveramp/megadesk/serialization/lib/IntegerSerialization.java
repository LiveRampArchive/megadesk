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

package com.liveramp.megadesk.serialization.lib;

import com.liveramp.megadesk.serialization.Serialization;

public class IntegerSerialization implements Serialization<Integer> {

  @Override
  public byte[] serialize(Integer data) {
    if (data == null) {
      return null;
    } else {
      return data.toString().getBytes();
    }
  }

  @Override
  public Integer deserialize(byte[] serializedData) {
    if (serializedData == null || serializedData.length == 0) {
      return null;
    } else {
      return Integer.valueOf(new String(serializedData));
    }
  }
}
