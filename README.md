# About the project?
The KafkaProp PoC was developed to prove out that we could leverage Kafka/Zookeeper to handle messaging of data from an AUTH environment to multiple LIVE environments concurrently/simultaneously.

# Video Overview
https://rebrand.ly/hclc-kafkaprop-demo

# Problem being solved
Typically we’d use STAGEPROP to propagate data from AUTH to LIVE environment.  The limitation of STAGEPROP is the one-to-one (a single AUTH to a single LIVE) nature of STAGEPROP.  This PoC aimed to syndicate data from one AUTH environment to multiple LIVE environments concurrently.

This PoC’s scope does not cover “Quick Publish” data.

# What is the solution
In this Proof of Concept, we created a WAS Java Producer and WAS Java Consumer.  We added the Producer in a modified TS-UTIL container in the AUTH environment.  We added the Consumer in a modified TS-UTIL container in the LIVE environment.  Kafka/Zookeeper sat in the middle between the two environments.  The AUTH TS-UTIL container created a sample CSV file using STAGEPROP, the Producer then sent that data to an existing Kafka topic “stageprop”.  The Consumer in the LIVE environment then consumed that data into the TS-UTIL container and then used DATALOAD to commit the data to the LIVE DB.

![Solution Diagram](images/Solution_Diagram_1.png)

# Setup instructions
There are two ways to setup the artifacts for this POC.
- [Setup for a development environment](setup-for-a-development-environment)
- [Setup with a Commerce runtime environment](setup-with-a-commerce-runtime-environment)


## Setup for a development environment

### Fixed and updated code examples from the book "Apache Kafka"

* Updated to Apache Kafka 0.8.1.1
* Configuration optimized for usage on Windows machines
* Windows batch scripts fixed (taken from https://github.com/HCanber/kafka by @HCanber)
* Code examples repaired and refactored

#### Initial Setup

1. [Download and install Apache Kafka](http://kafka.apache.org/downloads.html) 0.8.1.1 (I used the recommended [Scala 2.9.2 binary](https://www.apache.org/dyn/closer.cgi?path=/kafka/0.8.1.1/kafka_2.9.2-0.8.1.1.tgz))
2. Copy the scripts from [/bat](/bat) into `/bin/windows` of your Kafka installation folder (overwrite existing scripts)
3. Copy the property files from [/config](/config) into `/config` of your Kafka installation folder (overwrite existing files)

#### Simple Java Producer (Chapter 5, Page 35ff.)

1. Open command line in your Kafka installation folder
2. Launch Zookeeper with `.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties`
3. Open a second command line in your Kafka installation folder
4. Launch single Kafka broker: `.\bin\windows\kafka-server-start.bat .\config\server.properties`
5. Open a third command line in your Kafka installation folder
6. Create a topic: `.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test`
7. Start a console consumer for that topic: `.\bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic test --from-beginning`
8. From a fourth command line or your IDE run [SimpleProducer](/src/test/kafka/SimpleProducer.java) with topic and message as arguments: `java SimpleProducer test HelloKafka`
9. The message _HelloKafka_ should appear in the console consumer's log

#### Java Producer with Message Partitioning (Chapter 5, Page 37ff.)

1. Open command line in your Kafka installation folder
2. Launch Zookeeper with `.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties`
3. Open a second command line in your Kafka installation folder
4. Launch first Kafka broker: `.\bin\windows\kafka-server-start.bat .\config\server-1.properties`
5. Open a third command line in your Kafka installation folder
6. Launch second Kafka broker: `.\bin\windows\kafka-server-start.bat .\config\server-2.properties`
7. Open a fourth command line in your Kafka installation folder
8. Create a topic: `.\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 2 --partitions 4 --topic kafkatest`
9. Start a console consumer for that topic: `.\bin\windows\kafka-console-consumer.bat --zookeeper localhost:2181 --topic kafkatest --from-beginning`
10. From a fifth command line or your IDE run [MultiBrokerProducer](/src/test/kafka/MultiBrokerProducer.java) with topic as argument: `java MultiBrokerProducer kafkatest`
11. Ten messages starting with _This message is for key - (...)_ should appear in the console consumer's log

#### Simple High Level Java Consumer (Chapter 6, Page 47ff.)

1. Launch multi-broker Kafka cluster and create topic `kafkatest` as described in step 1-8 of __Java Producer with Message Partitioning__
2. From another command line or your IDE run [SimpleHLConsumer](/src/test/kafka/consumer/SimpleHLConsumer.java) with topic as argument: `java SimpleHLConsumer kafkatest`
3. From another command line or your IDE run [MultiBrokerProducer](/src/test/kafka/MultiBrokerProducer.java) with same topic as argument: `java MultiBrokerProducer kafkatest`
4. Ten messages starting with _This message is for key - (...)_ should appear in the log of the __SimpleHLConsumer__

#### Multithreaded Consumer for Multipartition Topics (Chapter 6, Page 50ff.)

1. Launch multi-broker Kafka cluster and create topic `kafkatest` as described in step 1-8 of __Java Producer with Message Partitioning__
2. From another command line or your IDE run [MultiThreadHLConsumer](/src/test/kafka/consumer/MultiThreadHLConsumer.java) with topic and number of threads as argument: `java MultiThreadHLConsumer kafkatest 4`
4. From another command line continuously produce messages by running [MultiBrokerProducer](/src/test/kafka/MultiBrokerProducer.java) several times in a row: `java MultiBrokerProducer kafkatest` (Note: You must start producing messages within 10sec after starting the consumer class, otherwise the consumer will shut down)
5. Messages starting with _Message from thread (...)_ should appear in the log of the __MultiThreadHLConsumer__ spread among the four threads

## Setup with a Commerce runtime environment
### Requirements:
- Commerce AUTH running in K8S
- Commerce LIVE running in K8S
- Kafka running in K8SDocker environment: needed to create a new util container

Using a combination of Docker and K8S we will setup the environment for ***KafkaProp***. To create a new _modified util container_ we will make use of Docker. Once the _new util image_ is created it is required to be pushed into a _docker registry_ and then we will use the _Helm Charts_ K8S to deploy the modified util containers.

#### Setup:
1. Download the util container
2. Using a running util container
3. Download and Install a java that is older than: 1.8.0_221, inside of the container
4. Create a folder inside of the container /kafkaprop/
5. Download to /kafkaprop/ the entire content of https://rebrand.ly/hcl-kafkaprop (with the exception of the helm charts)
6. Create a new layer for the util container and this will be your modified util container. Use `docker commit <IMAGE_TAG> <NEW_TAG>` 
7. Download the helm to your local k8s charts and modify the values.yaml to match your setup. You only need to be concern about the environment variables for the containers, and the version of the modified container
8. `helm install kafkaProp ./`

#### Run _stageprop.sh_ inside of the producer
1. using a modified version of the following command: `stagingprop.sh -scope _all_ -sourcedb 10.190.176.228:50001/mall -sourcedb_user wcs -sourcedb_passwd wcs1 -dbtype db2 -fileoutput 2 -filecommitsize 20 -fileoutputdirectory /data/`
2. `cd /kafkaprop/`
3. `./runProducer.sh`



#### Run _dataload_ inside of the Consumer:
1. `cd /kafkaProp/`
2. `./runConsumer.sh`
3. `mv /output/* /JOB/csv/`
4. `/opt/WebSphere/CommerceServer90/bin/dataload.sh /JOB/wc-dataload.xml -DXmlValidation=false`
