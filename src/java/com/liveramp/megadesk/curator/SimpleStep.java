package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.step.Step;
import com.netflix.curator.framework.CuratorFramework;

public class SimpleStep
    extends CuratorStep<Object, SimpleStep>
    implements Step<Object, SimpleStep> {

  public SimpleStep(CuratorFramework curator, String id) throws Exception {
    super(curator, id, null);
  }

  @Override
  public Object getData() {
    throw new IllegalStateException("A SimpleStep has no data.");
  }
}
