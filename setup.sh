#!/bin/bash

export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=8889"
export SPARK_LOCAL_IP="172.31.46.15"
export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"
export JAVA_HOME=~/.sdkman/candidates/java/current