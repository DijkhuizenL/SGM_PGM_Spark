# Subgraph matching on property graphs in a distributed setting

This is the Python code generated for my master thesis. Below, instructions of setting up the application and run queries is given. This guide is created for Linux.  

## Installation

### Java and Spark installation
First, verify if Java is installed. Version 1.8.x or higher is recommended.
`$java -version`
The following response is given if there is an installation present on your machine:
```
openjdk version "1.8.0_181"
OpenJDK Runtime Environment (build 1.8.0_181-b13)
OpenJDK 64-Bit Server VM (build 25.181-b13, mixed mode)
```
If not, install Java according to the instructions on the [Java website](https://www.java.com/).

The latest version of Spark can be downloaded [here](https://spark.apache.org/downloads.html). This application was built with **spark-3.0.0-bin-hadoop3.2**. When downloaded please make sure to unzip the tar:
`$ tar xvf spark-3.0.0-bin-hadoop3.2.tgz`
Subsequently, move the extracted folder to a location known to you. This is the $SPARK_HOME directory which is required later.

## Connecting the application
Several settings are need to be changed to run the application. These settings can be found in the `main.py` file.

To run the application in the PySpark environment, copy the path of the $SPARK_HOME directory inbetween the brackets of `findspark.init()` in line `15`.
The `sparkContext` requires a master URL and an application name. The master URL is currently set to `local[*]`, but allows connection to different master nodes. More information on master URLs and connections can be found [here](https://spark.apache.org/docs/latest/submitting-applications.html).

## Running the application
To run the application, set the `graph_filename` variable at line `286` in the `main.py` file, which is a path to the graph data file which you want to query.  To define the query, copy a valid LuuQL query in the `queries/query.txt` file.

When the Spark cluster is connected to the application and the data and query is defined, the application can be run in the `luuql_venv` virtual environment. The command: 
`luuql_venv/bin/activate` activates the environment, which is visible by the terminal showing `(luuql_venv)` on the commandline. Subsequently run the PySpark application by the command: `$python3 main.py`. 
