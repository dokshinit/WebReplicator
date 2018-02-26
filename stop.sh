#!/bin/sh

# Остановка службы (останавливаются все экземпляры).

name="WebReplicator.jar"
mm=`/usr/lib/jvm/default-java/bin/jps | grep $name`

for line in $mm; do
    if [ $line != "$name" ]; then
        echo "Found $name, pid=$line, stopping..."
        kill $line
    fi
done

