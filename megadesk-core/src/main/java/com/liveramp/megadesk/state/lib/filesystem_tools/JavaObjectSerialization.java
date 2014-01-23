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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class JavaObjectSerialization<T> implements SerializationHandler<T> {

  @Override
  public byte[] serialize(Object o) throws IOException {
    ByteArrayOutputStream bytesOutputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(bytesOutputStream);
    objectOutputStream.writeObject(o);
    objectOutputStream.close();
    return bytesOutputStream.toByteArray();
  }

  @Override
  public T deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bytesInputStream = new ByteArrayInputStream(bytes);
    ObjectInputStream objectInputStream = new ObjectInputStream(bytesInputStream);
    Object o;
    try {
      o = objectInputStream.readObject();
    } finally {
      objectInputStream.close();
    }
    return (T)o;
  }
}
