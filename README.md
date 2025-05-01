# Queryable storage

Simple queryable storage interface that supports read and write operations.
Contains an efficient JDBC implementation that loads query templates from properties files (see examples).

Build and install into local Maven repository:

```sh
mvn install
```

Run integration tests:

```sh
mvn -pl core test -Dtest=QueryableStorageIT
```

Run examples:

```sh
mvn -pl examples compile exec:java \
  -Dexec.mainClass="it.fvaleri.qstorage.examples.RunUsers"

mvn -pl examples compile exec:java \
  -Dexec.mainClass="it.fvaleri.qstorage.examples.RunPagamento"
```
