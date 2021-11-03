# RocksDemo

**Compiling**: `mvn package`

**Running**:

Ensure you have installed `cryptotool` into your local repo:

```
mvn install:install-file \
    -Dfile=target/cryptotool-0.1.jar \
    -DgroupId=net.pdutta.sandbox \
    -DartifactId=cryptotool \
    -Dversion=0.1 \
    -Dpackaging=jar -DcreateChecksum=true
```

Run `java -classpath target/rocksdemo-0.1.jar:target/lib/* net.pdutta.rocksdemo.App`
