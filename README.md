MIST: MIcro STream processing system
====================================

MIST is a micro stream processing system built on top of [Apache REEF](http://reef.apache.org/).

### Requirements
 - Java 1.8

### How to build and run MIST
1. Build MIST:
    ```
    git clone https://github.com/cmssnu/mist
    cd mist
    Set $MIST_HOME (export MIST_HOME=`pwd`)
    mvn clean install
    ```


2. Run MIST:
    ```
    ./bin/start.sh
    [-? Print help]
    -num_executors The number of MIST executors
    -num_task_cores The number of cores of MIST task
    [-num_tasks The number of mist tasks]
    [-port The port number of the RPC server]
    [-runtime Type of the MIST Driver runtime]
    -task_mem_size The size of task memory (MB)
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
    ./bin/start.sh -num_executors 1 -num_task_cores 1 -task_mem_size 1024
    # 2. Launch source server (You can simply use netcat)
     nc -lk 20331
    # 3. Run a script for HelloMist
    ./bin/run_hellomist.sh
    [-? Print help]
    [-d Address of running MIST driver in the form of hostname:port]
    [-s Address of running source server in the form of hostname:port]
    ```
