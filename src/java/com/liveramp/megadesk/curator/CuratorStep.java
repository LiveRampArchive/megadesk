package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.serialization.Serialization;
import com.liveramp.megadesk.step.BaseStep;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.step.StepLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

public class CuratorStep<T, SELF extends CuratorStep>
    extends BaseStep<T, SELF>
    implements Step<T, SELF> {

  private static final String STEPS_PATH = "/megadesk/steps";

  private final CuratorFramework curator;
  private final String path;
  private final Serialization<T> dataSerialization;
  private final CuratorStepLock lock;

  public CuratorStep(CuratorFramework curator,
                     String id,
                     Serialization<T> dataSerialization) throws Exception {
    super(id);
    this.curator = curator;
    this.path = ZkPath.append(STEPS_PATH, id);
    this.dataSerialization = dataSerialization;
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorStepLock(curator, path);
  }

  @Override
  protected StepLock getLock() {
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
