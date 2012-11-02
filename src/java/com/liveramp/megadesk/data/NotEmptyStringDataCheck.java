package com.liveramp.megadesk.data;

import com.liveramp.megadesk.resource.Resource;

public class NotEmptyStringDataCheck implements DataCheck<String> {

  @Override
  public boolean check(Resource<String> resource) throws Exception {
    String data = resource.getData();
    return data != null && data.length() > 0;
  }
}
