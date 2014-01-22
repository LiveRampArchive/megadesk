package com.liveramp.megadesk.state.lib.filesystem_tools;

public interface PathMaker {

  public String makePath(String name);

  public static class Default implements PathMaker {
    private final String root;

    public Default(String root) {
      this.root = root;
    }

    @Override
    public String makePath(String name) {
      return root + "/" + name;
    }
  }
}
