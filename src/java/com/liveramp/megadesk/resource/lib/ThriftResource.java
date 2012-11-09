package com.liveramp.megadesk.resource.lib;

import com.liveramp.megadesk.driver.ResourceDriver;
import com.liveramp.megadesk.resource.BaseResource;
import com.liveramp.megadesk.resource.Resource;
import com.liveramp.megadesk.serialization.lib.ThriftJsonSerialization;
import org.apache.thrift.TBase;

public class ThriftResource<T extends TBase>
    extends BaseResource<T>
    implements Resource<T> {

  protected ThriftResource(String id,
                           ResourceDriver driver,
                           T baseObject) {
    super(id, driver, new ThriftJsonSerialization<T>(baseObject));
  }
}
