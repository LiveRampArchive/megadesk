package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.maneuver.BaseManeuver;
import com.liveramp.megadesk.maneuver.Maneuver;
import com.liveramp.megadesk.maneuver.ManeuverLock;
import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

import java.util.List;

public class CuratorManeuver extends BaseManeuver implements Maneuver {

  private static final String STEPS_PATH = "/maneuvers";

  private final CuratorManeuverLock lock;

  public CuratorManeuver(CuratorFramework curator,
                         String id,
                         List<Read> reads,
                         List<Resource> writes) throws Exception {
    super(id, reads, writes);
    String path = ZkPath.append(STEPS_PATH, id);
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorManeuverLock(curator, path);
  }

  @Override
  protected ManeuverLock getLock() {
    return lock;
  }
}
