MIST: High-performance IoT Stream Processing
====================================

MIST is a stream processing system that is optimized to handle large numbers of IoT stream queries. 
MIST is built on top of [Apache REEF](http://reef.apache.org/).

### Requirements
 - Java 1.8
 - Maven
 
### How to build and run MIST
1. Build MIST:

    ```
    git clone https://github.com/snuspl/mist
    cd mist
    Set $MIST_HOME (export MIST_HOME=`pwd`)
    mvn clean install
    ```

2. Run MIST:
 
    ```
    ./bin/start.sh
    [-? Print help]
    -num_threads The number of MIST threads
    -num_task_cores The number of cores of MIST task
    [-num_tasks The number of mist tasks]
    [-port The port number of the RPC server that communicates with MIST client]
    [-runtime Type of the MIST Driver runtime (yarn or local. The default value is local runtime)]
    -task_mem_size The size of MIST task memory (MB)
    ```
    

### MIST examples
#### HelloMist

1. Description

    The example shows how to implement a simple stateless query and submit it to the MIST.
    The code sets the environment(Hostnames and ports of MIST, source, sink), generates a simple stateless query, and submits it to the MIST.
    The query reads strings from a source server, filter strings which start with "HelloMIST:", trim "HelloMIST:" part of the filtered strings, and send them to a sink server.

2. Run HelloMist

    ```
    # 0. Build MIST first!(See above)
    # 1. Run MIST
    ./bin/start.sh -num_threads 1 -num_task_cores 1 -task_mem_size 1024
    # 2. Launch source server (You can simply use netcat)
     nc -lk 20331
    # 3. Run a script for HelloMist
    ./bin/run_example.sh HelloMist
    [-? Print help]
    [-d Address of running MIST driver in the form of hostname:port]
    [-s Address of running source server in the form of hostname:port]

    # 4. Publish data stream 
     nc -lk 20331
     HelloMIST: hello!  (Then HelloMist query will filter this string and print "hello!" message to the console) 
    ```

#### Multi-query submission  

 To submit stream queries, application developers first submit their jar files to MIST (The HelloMist example automatically submits the jar file and query to MIST). 
   
```
# submit jar file
./bin/submit_jar.sh ./mist-examples/target/mist-examples-0.2-SNAPSHOT.jar
```

Then, it will return the app identifier

```
App identifier: 0
```

We can use this app identifier to submit multiple stream queries of the app. We will use [EMQ](http://www.emqttd.io) as an MQTT message broker. Please download the EMQ and start it. 
    
```
emqttd start
```
    
The default port of the emqttd that subscribes and publishes data is 1883. 
We then submit a stream query that filters data according to the user-defined prefix. This query subscribes/publishes the data stream from/to the EMQ.  

```
./bin/run_example.sh MqttFilterApplication -source_topic /src/1 -sink_topic /sink/1 -filtered_string "HelloMIST:" -app_id 0
```
    
Then, the query will subscribe data from `/src/1` EMQ topic, filter data that contains `HelloMIST:` as a prefix, and publishes the data to `/sink/1` EMQ topic.
    
To publish and subscribe the topic, we can use [mosquitto_pub](https://mosquitto.org/man/mosquitto_pub-1.html) and [mosquitto_sub](https://mosquitto.org/man/mosquitto_sub-1.html).
 We can subscribe the `/sink/1` topic by:   
 
```
mosquitto_sub -h 127.0.0.1 -t /sink/1
```
    
We can publish the `/src/1` topic by :
    
```
mosquitto_pub -h 127.0.0.1 -t /src/1 -m "HelloMIST: hello!"
```

We can submit another stream query that filters `HelloWorld:` 

```
./bin/run_example.sh MqttFilterApplication -source_topic /src/2 -sink_topic /sink/2 -filtered_string "HelloWorld:" -app_id 0
```
    
