package com.liveramp.megadesk.resource;

import java.util.Arrays;
import java.util.List;

public class Reads {

  public static List<Read> list(Read... reads) {
    return Arrays.asList(reads);
  }
}
