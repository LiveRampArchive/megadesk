package com.liveramp.megadesk.curator;

import com.liveramp.megadesk.serialization.StringSerialization;
import com.liveramp.megadesk.step.Step;
import com.netflix.curator.framework.CuratorFramework;

public class StringStep
    extends CuratorStep<String, StringStep>
    implements Step<String, StringStep> {

  public StringStep(CuratorFramework curator, String id) throws Exception {
    super(curator, id, new StringSerialization());
  }
}
