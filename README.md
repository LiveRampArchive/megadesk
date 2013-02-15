Project Megadesk
========

Download
====
You can either build megadesk from source as described below, or pull the latest jar from the Liveramp Maven repository:

```xml
<repository>
  <id>repository.liveramp.com</id>
  <name>liveramp-repositories</name>
  <url>http://repository.liveramp.com/artifactory/liveramp-repositories</url>
</repository>
```

The 1.0-SNAPSHOT build can be retrieved there:

```xml
<dependency>
  <groupId>com.liveramp</groupId>
  <artifactId>megadesk</artifactId>
  <version>0.1-SNAPSHOT</version>
</dependency>
```

Building
====

To build megadesk from source,

```bash
> mvn package
```

will generate the jar in target/.  To run the test suite locally,

```bash
> mvn test
```

