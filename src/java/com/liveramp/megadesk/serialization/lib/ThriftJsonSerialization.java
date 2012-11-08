package com.liveramp.megadesk.serialization.lib;

import com.liveramp.megadesk.serialization.Serialization;
import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TJSONProtocol;

public class ThriftJsonSerialization<T extends TBase> extends ThriftSerialization<T> implements Serialization<T> {

  public ThriftJsonSerialization(T baseObject) {
    super(new TJSONProtocol.Factory(), baseObject);
  }
}
