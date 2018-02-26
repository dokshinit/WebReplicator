#!/bin/sh

# Запуск в виде консольного приложения с интерфейсом отображения.

# Копирование собранного релиза в тек.каталог (в релизе убрать!).
cp ./out/artifacts/WebReplicator_jar/WebReplicator.jar ./WebReplicator.jar

java -jar ./WebReplicator.jar showui