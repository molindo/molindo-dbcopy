molindo-dbcopy
==============

Incrementally compare and synchronize MySQL databases.

Usage:
------

    wget [$URL](https://oss.sonatype.org/service/local/repositories/snapshots/content/at/molindo/molindo-dbcopy/0.1.0-SNAPSHOT/molindo-dbcopy-0.1.0-20140210.131708-1.jar)
    cat > dbcopy.properties <<PROPS
    source.jdbc=jdbc:mysql://localhost/db1
    source.user=user1
    source.pass=pw1
    source.pool=2
    
    target.jdbc=jdbc:mysql://localhost/db2
    target.user=user2
    target.pass=pw2
    target.pool=4
    
    db.dry_run=true
    db.disable_unique_checks=true
    
    task.tables.include=*
    
    task.queries.q1.query=select ... order by ...
    task.queries.q1.table=Q1_TARGET
    task.queries.q2.query=select ... order by ...
    task.queries.q2.table=Q2_TARGET
    PROPS
    
    chmod +x molindo-dbcopy-*.jar
    java -jar molindo-dbcopy-*.jar

Maven:
------

    <dependency>
      <groupId>at.molindo</groupId>
      <artifactId>molindo-dbcopy</artifactId>
      <version>0.1.0</version>
    </dependency>

Releases available from Maven Central, snapshots from [oss.sonatype.org](https://oss.sonatype.org/index.html#nexus-search;gav~at.molindo~molindo-dbcopy~~jar~).

CI Build:
---------

[![Build Status](https://travis-ci.org/molindo/molindo-dbcopy.png)](https://travis-ci.org/molindo/molindo-dbcopy)

