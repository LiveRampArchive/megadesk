package com.liveramp.megadesk.state.lib.filesystem_tools;

import java.io.IOException;

public interface SerializationHandler<T> {

  public byte[] serialize(T o) throws IOException;

  public T deserialize(byte[] bytes) throws IOException;
}
