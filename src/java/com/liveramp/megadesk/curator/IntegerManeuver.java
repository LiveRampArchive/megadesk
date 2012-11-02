package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.maneuver.Maneuver;
import com.liveramp.megadesk.serialization.IntegerSerialization;
import com.netflix.curator.framework.CuratorFramework;

public class IntegerManeuver
    extends CuratorManeuver<Integer, IntegerManeuver>
    implements Maneuver<Integer, IntegerManeuver> {

  public IntegerManeuver(CuratorFramework curator, String id) throws Exception {
    super(curator, id, new IntegerSerialization());
  }
}
