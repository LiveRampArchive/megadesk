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

package com.liveramp.megadesk.recipes.state.persistence;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JavaObjectSerializationHandler<T> implements SerializationHandler<T> {

  @Override
  public byte[] serialize(T value) throws IOException {
    ByteArrayOutputStream bytesOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(bytesOutputStream);
    objectOutputStream.writeObject(value);
    objectOutputStream.close();
    return bytesOutputStream.toByteArray();
  }

  @Override
  public T deserialize(byte[] serializedValue) throws IOException {
    if (serializedValue.length > 0) {
      ByteArrayInputStream bytesInputStream = new ByteArrayInputStream(serializedValue);
      ObjectInputStream objectInputStream = new ObjectInputStream(bytesInputStream);
      Object object;
      try {
        try {
          object = objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
          throw new IOException(e);
        }
      } finally {
        objectInputStream.close();
      }
      return (T)object;
    } else {
      return null;
    }
  }
}
