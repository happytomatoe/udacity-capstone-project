#!/bin/bash


#TODO: add to .bashrc
export JAVA_HOME=/home/babkamen/.sdkman/candidates/java/current
export SPARK_HOME=/home/babkamen/Documents/spark-3.2.0-bin-hadoop3.2
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
#export PYSPARK_DRIVER_PYTHON="jupyter"
#export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON=python3
export PATH=$SPARK_HOME/bin:$PATH
