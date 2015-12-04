# Zeppelin MySQL

MySQL plugin for [Apache Zeppelin](https://zeppelin.incubator.apache.org) based on the PostgreSQL plugin.

It allows you to run MySQL SQL queries within Apache Zeppelin.

## Building

Right now there aren't packages pre-built.

1. Pull down `zeppelin-0.5.5-incubating-bin-all` from `https://zeppelin.incubator.apache.org`
2. Clone the git repo at `https://github.com/apache/incubator-zeppelin/`, switch to the `branch-0.5.5` and run `mvn install`
3. Pull down the `zeppelin-mysql` repository and run `mvn package`
4. Copy `zeppelin-mysql/target/interpreter/mysql` to `zeppelin-0.5.5-incubating-bin-all/interpreter`
5. Edit `zeppelin-0.5.5-incubating-bin-all/conf/zeppelin-site.xml` and add `org.apache.zeppelin.mysql.MySqlInterpreter` to the list of `zeppelin.interpreters`

## Testing

`mvn test`

