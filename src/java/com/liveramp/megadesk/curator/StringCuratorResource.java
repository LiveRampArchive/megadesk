package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.state.EqualityStateCheck;
import com.liveramp.megadesk.state.StringStateSerialization;
import com.netflix.curator.framework.CuratorFramework;

public class StringCuratorResource extends CuratorResource<String> implements Resource<String> {

  public StringCuratorResource(CuratorFramework curator,
                               String id) throws Exception {
    super(curator, id, new StringStateSerialization());
  }

  public Read is(String state) {
    return new Read(this, new EqualityStateCheck<String>(state));
  }
}
