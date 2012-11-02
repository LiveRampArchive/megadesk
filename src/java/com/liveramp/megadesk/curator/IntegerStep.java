package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.step.Step;
import com.liveramp.megadesk.serialization.IntegerSerialization;
import com.netflix.curator.framework.CuratorFramework;

public class IntegerStep
    extends CuratorStep<Integer, IntegerStep>
    implements Step<Integer, IntegerStep> {

  public IntegerStep(CuratorFramework curator, String id) throws Exception {
    super(curator, id, new IntegerSerialization());
  }
}
