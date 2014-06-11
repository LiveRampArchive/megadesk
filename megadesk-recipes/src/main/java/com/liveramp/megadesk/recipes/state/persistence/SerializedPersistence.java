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

import java.io.IOException;

import com.liveramp.megadesk.base.state.BasePersistence;
import com.liveramp.megadesk.core.state.Persistence;
import com.liveramp.megadesk.core.state.PersistenceTransaction;

public abstract class SerializedPersistence<VALUE> extends BasePersistence<VALUE> implements Persistence<VALUE> {

  private final SerializationHandler<VALUE> serializationHandler;

  protected SerializedPersistence(SerializationHandler<VALUE> serializationHandler) {
    this.serializationHandler = serializationHandler;
  }

  @Override
  public VALUE read() {
    byte[] data = readBytes();
    VALUE value;
    try {
      value = serializationHandler.deserialize(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    return value;
  }

  @Override
  public void write(VALUE value) {
    try {
      writeBytes(serializationHandler.serialize(value));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeInTransaction(PersistenceTransaction transaction, VALUE value) {
    try {
      writeInTransaction(transaction, serializationHandler.serialize(value));
    } catch (IOException e) {
      throw new RuntimeException(e); // TODO
    }
  }

  protected abstract byte[] readBytes();

  protected abstract void writeBytes(byte[] serializedValue);

  public abstract void writeInTransaction(PersistenceTransaction transaction, byte[] serializedValue);
}
