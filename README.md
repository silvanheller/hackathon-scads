# Project "Ring Parable"

This repository contains the stream processing code of the "Ring Parable" project by Silvan Heller, Ashery Mbilinyi and Lukas Probst.

The goal of this project was to analyze how religions in different countries are represented in the dataset in terms of the average tone, goldstein metric and quad class.

The stream processing part of the project was implemented using Flink and Scala.

Aggregate stream processing results are then visualized using a web-based UI which you can find on [Github](https://github.com/silvanheller/hackathon-scads-ui).

The structure of both the repository and the code is base on the boilerplate code provided on [Github](https://github.com/TU-Berlin-DIMA/streamline-hackathon-boilerplate).

We used the GDELT Dataset provided by the organizers of the Hackathon [1].

## Overview
We aggregate the goldstein, avgTone and quadClass measure over religion-country combinations for both actor 1 and 2. Aggregate results are stored in the `storage/` folder.

## Run The Code locally (Option 1)

You may run the code from IntelliJ using the same entrypoint as in the boilerplate code - `eu.streamline.hackathon.flink.scala.job.FlinkScalaJob`

## Run The Code on your cluster (Option 2)

Compile the code by executing on the root directory of this repository:
```
mvn clean package
```

After that, you need to submit the job to Flink Job Manager.
Please, be sure that a standalone (or cluster) version of Flink is running on your machine as explained here [2].

Start the job: 
```
/path/to/flink/root/bin/flink run \
hackathon-flink-scala/target/hackathon-flink-scala-0.1-SNAPSHOT.jar \
--path /path/to/data/180-days.csv
```

## Fast testing & Development
Executing the code takes ~10-15 minutes on a Lenovo X1-Carbon 2017. If you do not want to wait this long, we provide a massively reduced dataset (just 10k events) at `storage/10k.csv`. This works as a drop-in replacement for the original dataset.

## References
[1] GDELT Projet: https://www.gdeltproject.org

[2] https://ci.apache.org/projects/flink/flink-docs-release-1.5/quickstart/setup_quickstart.html
