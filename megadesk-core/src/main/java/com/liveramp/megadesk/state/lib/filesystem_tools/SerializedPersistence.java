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

package com.liveramp.megadesk.state.lib.filesystem_tools;

import java.io.IOException;

import com.liveramp.megadesk.state.Persistence;
import com.liveramp.megadesk.state.lib.BasePersistence;

public abstract class SerializedPersistence<VALUE> extends BasePersistence<VALUE> implements Persistence<VALUE> {
  private final SerializationHandler<VALUE> serializer;

  protected SerializedPersistence(SerializationHandler<VALUE> serializer) {
    this.serializer = serializer;
  }

  @Override
  public VALUE read() {
    byte[] data = readBytes();
    VALUE value;
    try {
      value = serializer.deserialize(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return value;
  }

  @Override
  public void write(VALUE value) {
    try {
      writeBytes(serializer.serialize(value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract void writeBytes(byte[] serializedObject);

  protected abstract byte[] readBytes();
}
