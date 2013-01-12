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

import java.io.IOException;

public class EnumSerialization<T extends Enum<T>> implements Serialization<T> {

  private final Class<T> clazz;

  public EnumSerialization(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public byte[] serialize(T object) throws IOException {
    return object.toString().getBytes();
  }

  @Override
  public T deserialize(byte[] serializedObject) throws IOException {
    return T.valueOf(clazz, new String(serializedObject));
  }
}
