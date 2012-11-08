package com.liveramp.megadesk.serialization.lib;

import com.liveramp.megadesk.serialization.Serialization;
import org.yaml.snakeyaml.Yaml;

public class YamlSerialization implements Serialization<Object> {

  private final Yaml yaml = new Yaml();

  @Override
  public byte[] serialize(Object object) {
    if (object == null) {
      return null;
    } else {
      return yaml.dump(object).getBytes();
    }
  }

  @Override
  public Object deserialize(byte[] serializedObject) {
    if (serializedObject == null) {
      return null;
    } else {
      return yaml.load(new String(serializedObject));
    }
  }
}
