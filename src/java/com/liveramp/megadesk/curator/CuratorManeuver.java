package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.maneuver.BaseManeuver;
import com.liveramp.megadesk.maneuver.Maneuver;
import com.liveramp.megadesk.maneuver.ManeuverLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

public class CuratorManeuver extends BaseManeuver implements Maneuver {

  private static final String MANEUVERS_PATH = "/maneuvers";

  private final CuratorManeuverLock lock;

  public CuratorManeuver(CuratorFramework curator,
                         String id) throws Exception {
    super(id);
    String path = ZkPath.append(MANEUVERS_PATH, id);
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorManeuverLock(curator, path);
  }

  @Override
  protected ManeuverLock getLock() {
    return lock;
  }
}
