package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.maneuver.BaseManeuver;
import com.liveramp.megadesk.maneuver.Maneuver;
import com.liveramp.megadesk.maneuver.ManeuverLock;
import com.liveramp.megadesk.serialization.Serialization;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

public class CuratorManeuver<T, CRTP extends CuratorManeuver>
    extends BaseManeuver<T, CRTP>
    implements Maneuver<T, CRTP> {

  private static final String MANEUVERS_PATH = "/maneuvers";

  private final CuratorFramework curator;
  private final String path;
  private final Serialization<T> statusSerialization;
  private final CuratorManeuverLock lock;

  public CuratorManeuver(CuratorFramework curator,
                         String id,
                         Serialization<T> statusSerialization) throws Exception {
    super(id);
    this.curator = curator;
    this.path = ZkPath.append(MANEUVERS_PATH, id);
    this.statusSerialization = statusSerialization;
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorManeuverLock(curator, path);
  }

  @Override
  protected ManeuverLock getLock() {
    return lock;
  }

  @Override
  public T getStatus() throws Exception {
    // TODO: locking
    return statusSerialization.deserialize(curator.getData().forPath(path));
  }

  @Override
  public void setStatus(T status) throws Exception {
    // TODO: locking
    curator.setData().forPath(path, statusSerialization.serialize(status));
  }
}
