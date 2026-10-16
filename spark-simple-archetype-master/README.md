[![Maven Central](https://img.shields.io/maven-central/v/fr.uha.ensisa.ff/spark-simple-archetype.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22fr.uha.ensisa.ff%22%20AND%20a%3A%22spark-simple-archetype%22)

# spark-simple-archetype

## Creating a project

Simple maven archetype to startup a Java-based Apache Spark project.

To create your project:

```
mvn archetype:generate \
  -DgroupId=your.group \
  -DartifactId=your_project \
  -DarchetypeGroupId=fr.uha.ensisa.ff \
  -DarchetypeArtifactId=spark-simple-archetype \
  -DarchetypeVersion=0.0.8 \
  -DinteractiveMode=false
```

## Compile project

To compile your project: `mvn clean verify`

To run locally (once compiled): `java -jar target/your_project-*.jar`

In case you're running a JDK &ge; 16 : `java --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED --add-opens java.base/java.net=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/java.lang.invoke=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED -jar target/your_project-*.jar`

# Running a cluster using docker swarm

You need to run commands using a bash command line (for windows user, use git bash).

## Getting a docker swarm using Vagrant

First install Vagrant and VirtualBox.

The following fires a 3 node cluster (can be changed using the NODES environment variable).

```
git clone --depth=1 https://github.com/fondemen/docker-swarm-vagrant.git
cd docker-swarm-vagrant
vagrant up
until vagrant ssh node01 -c 'etcdctl cluster-health' ; do sleep 1; done
SWARM=on vagrant up
```
You might need to select an ethernet interface to connect to ; usually, the first one is best.

To re-use an previously started-then-stopped cluster

```
cd docker-swarm-vagrant
vagrant up
```
You might need to `vagrant destroy -f` and retry in case the above fails.

Please, check the [docker-swarm-vagrant](https://github.com/fondemen/docker-swarm-vagrant) project for more details on setting up the swarm cluster.

## Uploading jar to Vagrant node

The easiest is to install the scp plugin: `vagrant plugin install vagrant-scp`
Compile you project as [explained above](#compile-project)

```
for m in $(vagrant status --machine-readable | grep ',state,' | cut -d, -f2); do vagrant scp ../your_project/target/your_project-*.jar $m:~/ana.jar; done
```

## Firing up a spark cluster

Not ready for production !

In case you use the Vagrant approach, login to the first node : `vagrant ssh`.

```
docker network create -d overlay spark-nw
docker service create --name spark-master --replicas 1 --network spark-nw  -e INIT_DAEMON_STEP=setup_spark -p 8080:8080 bde2020/spark-master:3.1.1-hadoop3.2-java11
docker service create --name spark-worker --mode global --network spark-nw  -e "SPARK_MASTER=spark://spark-master:7077" --limit-memory 1280M bde2020/spark-worker:3.1.1-hadoop3.2-java11
```

The master (and only the master) is visible at http://192.168.59.99:8080.
To see also workers activity, use a proxy such as Sqid :

```
docker service create --name squid --replicas 1 --network spark-nw -p 3128:3128 chrisdaish/squid
```

Then configure your browser to use an HTTP proxy server with host `192.168.59.99` and port `3128`.

## Submitting your Spark analysis

In case you use the Vagrant approach, you must have [uploaded](#uploading-jar-to-vagrant-node) your Spark program and logged in to node01 using `vagrant ssh`.

We assume here that your Spark analysis is available on ./ana.jar.

Change the main class path in the following command (don't forget to change main class - [your.group.Main]):

```
docker service create --name ana -p 4040:4040 --network spark-nw --restart-condition none -e ENABLE_INIT_DAEMON=false -e SPARK_APPLICATION_JAR_NAME=ana -e SPARK_APPLICATION_JAR_LOCATION=/app/ana.jar -e SPARK_APPLICATION_MAIN_CLASS=[your.group.Main] --mount source=$(pwd)/ana.jar,target=/app/ana.jar,type=bind bde2020/spark-submit:3.1.1-hadoop3.2-java11
```

You can read your application driver logs: `docker service logs -f ana`

Once finished, delete the ana service:  `docker service rm ana`

## Stopping the Vagrant swarm cluster

Exit the node01 node (`exit` command).

```
vagrant destroy -f
```
