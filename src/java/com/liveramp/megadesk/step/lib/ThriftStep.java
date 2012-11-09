package com.liveramp.megadesk.step.lib;

import com.liveramp.megadesk.Megadesk;
import com.liveramp.megadesk.serialization.lib.ThriftJsonSerialization;
import com.liveramp.megadesk.step.BaseStep;
import com.liveramp.megadesk.step.Step;
import org.apache.thrift.TBase;

public class ThriftStep<T extends TBase>
    extends BaseStep<T, ThriftStep<T>>
    implements Step<T, ThriftStep<T>> {

  public ThriftStep(String id,
                    Megadesk megadesk,
                    T baseObject) throws Exception {
    super(id, megadesk, new ThriftJsonSerialization<T>(baseObject));
  }
}
