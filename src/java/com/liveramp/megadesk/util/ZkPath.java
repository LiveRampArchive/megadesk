package com.liveramp.megadesk.util;

public class ZkPath {

  public static String append(String... parts) {
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < parts.length; ++i) {
      if (i != 0) {
        builder.append("/");
      }
      builder.append(parts[i]);
    }
    return builder.toString();
  }
}
