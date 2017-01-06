#!/bin/sh
#
# Copyright (C) 2017 Seoul National University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# This code starts HelloMist
# You should set the $MIST_HOME variable.

SELF_JAR=`echo $MIST_HOME/target/mist-*-shaded.jar`

CLASSPATH=$YARN_HOME/share/hadoop/common/*:$YARN_HOME/share/hadoop/common/lib/*:$YARN_HOME/share/hadoop/yarn/*:$YARN_HOME/share/hadoop/hdfs/*:$YARN_HOME/share/hadoop/mapreduce/lib/*:$YARN_HOME/share/hadoop/mapreduce/*

YARN_CONF_DIR=$YARN_HOME/etc/hadoop

CMD="java -cp $YARN_CONF_DIR:$SELF_JAR:$CLASSPATH $LOCAL_RUNTIME_TMP $LOGGING_CONFIG edu.snu.mist.examples.$*"

case $1 in
  HelloMist) ;; 
  QueryDeletion) ;;
  StopAndResume) ;;
  UnionMist) ;;
  WordCount) ;;
  WindowAndAggregate) ;;
  JoinAndApplyStateful) ;;
  KMeansClustering) ;;
  KafkaSource) ;;
  *)
    echo "Invalid input. Here is an example for using this script."
    echo "If you want to run HelloMIST with sink source option, please type like below."
    echo "$MIST_HOME/bin/run_example.sh HelloMist -s yourHost:yourPort"
    exit 1;;
esac
   
echo $CMD
$CMD
