package com.liveramp.megadesk.data;

import com.liveramp.megadesk.resource.Resource;

public interface DataCheck<T> {

  public boolean check(Resource<T> resource) throws Exception;
}
