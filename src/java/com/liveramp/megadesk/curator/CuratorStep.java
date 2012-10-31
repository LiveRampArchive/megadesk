package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.step.BaseStep;
import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.step.StepLock;
import com.netflix.curator.framework.CuratorFramework;

import java.util.List;

public class CuratorStep extends BaseStep implements Step {

  private final CuratorStepLock lock;

  public CuratorStep(CuratorFramework curator,
                     String path,
                     List<Resource> reads,
                     List<Resource> writes) {
    super(path, reads, writes);
    this.lock = new CuratorStepLock(curator, path);
  }

  @Override
  public StepLock getStepLock() {
    return lock;
  }
}
