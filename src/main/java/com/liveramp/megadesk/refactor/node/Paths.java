/**
 *  Copyright 2014 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.megadesk.refactor.node;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;

public final class Paths {

  public static final String SEPARATOR = "/";

  private Paths() {
  }

  public static String append(String... parts) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < parts.length; ++i) {
      builder.append(SEPARATOR);
      builder.append(parts[i]);
    }
    return sanitize(builder.toString());
  }

  public static String[] split(String path) {
    return StringUtils.split(sanitize(path), SEPARATOR);
  }

  public static Path parent(Path path) {
    return new Path(parent(path.get()));
  }

  public static String parent(String path) {
    String[] splits = split(path);
    if (splits.length <= 1) {
      return SEPARATOR;
    }
    return append(Arrays.copyOfRange(splits, 0, splits.length - 1));
  }

  public static String sanitize(String path) {
    // Replace multiple separators with one separator
    return path.replaceAll(SEPARATOR + "+", SEPARATOR);
  }
}
