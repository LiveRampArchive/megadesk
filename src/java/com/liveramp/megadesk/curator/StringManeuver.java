package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.maneuver.Maneuver;
import com.liveramp.megadesk.serialization.StringSerialization;
import com.netflix.curator.framework.CuratorFramework;

public class StringManeuver
    extends CuratorManeuver<String, StringManeuver>
    implements Maneuver<String, StringManeuver> {

  public StringManeuver(CuratorFramework curator, String id) throws Exception {
    super(curator, id, new StringSerialization());
  }
}
