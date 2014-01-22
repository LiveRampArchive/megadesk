package com.liveramp.megadesk.state.lib.filesystem_tools;

import java.io.IOException;

public interface SerializationHandler {

  public byte[] serialize(Object o) throws IOException;

  public Object deserialize(byte[] bytes) throws IOException;
}
