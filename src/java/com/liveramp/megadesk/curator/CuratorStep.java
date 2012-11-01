package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.Read;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.step.BaseStep;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.step.StepLock;
import com.liveramp.megadesk.util.ZkPath;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.utils.EnsurePath;

import java.util.List;

public class CuratorStep extends BaseStep implements Step {

  private static final String STEPS_PATH = "/steps";

  private final CuratorStepLock lock;

  public CuratorStep(CuratorFramework curator,
                     String id,
                     List<Read> reads,
                     List<Resource> writes) throws Exception {
    super(id, reads, writes);
    String path = ZkPath.append(STEPS_PATH, id);
    new EnsurePath(path).ensure(curator.getZookeeperClient());
    this.lock = new CuratorStepLock(curator, path);
  }

  @Override
  protected StepLock getLock() {
    return lock;
  }
}
