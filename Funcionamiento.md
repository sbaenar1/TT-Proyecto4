# Implementación

### Requisitos

Se debe tener Kafka y Apache Spark instalado en la máquina para que funcione.
Kafka se puede descargar desde [aquí]( https://kafka.apache.org/downloads.html)
Apache Spark se puede descargar desde [aquí](https://spark.apache.org/downloads.html)

Se requiere tener las librerias de Python instaladas, estas se encuentra en requisitos.txt y pueden ser instalada con `sudo pip install -r requisitos.txt`

### Correr Kafka
Para iniciar el servicio de Zookeeper:
`$ bin/zookeeper-server-start.sh config/zookeeper.properties`

Para iniciar el servicio de Kafka:
`$ bin/kafka-server-start.sh config/server.properties`

Crear un topic llamado twitterstream en kafka:
`$ bin/kafka-topics.sh --create --topic twitterstream --zookeeper localhost:2181 --partitions 1 --replication-factor 1`

### Correr el programa
Corre el script de python:
`$ python app.py`

Para verificar que los datos si lleguen a Kafka:
`$ bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitterstream --from-beginning`

Correr Spark Streaming:
`$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 twitterStream.py`