package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.maneuver.Maneuver;
import com.netflix.curator.framework.CuratorFramework;

public class SimpleManeuver
    extends CuratorManeuver<Object, SimpleManeuver>
    implements Maneuver<Object, SimpleManeuver> {

  public SimpleManeuver(CuratorFramework curator, String id) throws Exception {
    super(curator, id, null);
  }

  @Override
  public Object getData() {
    throw new IllegalStateException("A SimpleManeuver has no data.");
  }
}
