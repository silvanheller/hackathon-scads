# Project "Ring Parable"

This repository contains the stream processing code of the "Ring Parable" Project by Silvan Heller, Ashery Mbilinyi and Lukas Probst.

We have chosen to use the Flink & Spark combination.

The structure of both the repository and the code is base on the boilerplate code provided on [Github](https://github.com/TU-Berlin-DIMA/streamline-hackathon-boilerplate)

We used the GDELT Dataset provided by the organizers of the Hackathon [1]

## Run The Code locally (Option 1)

You may run the code from IntelliJ using the same entrypoint as in the boilerplate code.

## Run The Code on your cluster (Option 2)

Compile the code by executing on the root directory of this repository:
```
mvn clean package
```

After that, you need to submit the job to Flink Job Manager.
Please, be sure that a standalone (or cluster) version of Flink is running on your machine as explained here [2].

Start the job: 
```
# Scala Job
/path/to/flink/root/bin/flink run \
hackathon-flink-scala/target/hackathon-flink-scala-0.1-SNAPSHOT.jar \
--path /path/to/data/180-days.csv
```

## References
[1] GDELT Projet: https://www.gdeltproject.org

[2] https://ci.apache.org/projects/flink/flink-docs-release-1.5/quickstart/setup_quickstart.html
