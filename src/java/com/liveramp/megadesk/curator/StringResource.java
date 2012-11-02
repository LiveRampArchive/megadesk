package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.data.EqualityDataCheck;
import com.liveramp.megadesk.data.NotEmptyStringDataCheck;
import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.serialization.StringSerialization;
import com.netflix.curator.framework.CuratorFramework;

public class StringResource extends CuratorResource<String> implements Resource<String> {

  public StringResource(CuratorFramework curator,
                        String id) throws Exception {
    super(curator, id, new StringSerialization());
  }

  public Read at(String data) {
    return new Read<String>(this, new EqualityDataCheck<String>(data));
  }

  public Read notEmpty() {
    return new Read<String>(this, new NotEmptyStringDataCheck());
  }
}
