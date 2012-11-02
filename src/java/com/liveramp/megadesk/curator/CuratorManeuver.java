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
  private final Serialization<T> dataSerialization;
  private final CuratorManeuverLock lock;

  public CuratorManeuver(CuratorFramework curator,
                         String id,
                         Serialization<T> dataSerialization) throws Exception {
    super(id);
    this.curator = curator;
    this.path = ZkPath.append(MANEUVERS_PATH, id);
    this.dataSerialization = dataSerialization;
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorManeuverLock(curator, path);
  }

  @Override
  protected ManeuverLock getLock() {
    return lock;
  }

  @Override
  protected T doGetData() throws Exception {
    return dataSerialization.deserialize(curator.getData().forPath(path));
  }

  @Override
  protected void doSetData(T data) throws Exception {
    curator.setData().forPath(path, dataSerialization.serialize(data));
  }
}
