# Project "Ring Parable"

This repository contains the stream processing code of the "Ring Parable" project by Silvan Heller, Ashery Mbilinyi and Lukas Probst.

All over the world, people are biased and have prejudices based on their religion and culture. This induces two key questions:

* Are there patterns in the interactions between religions in general or religions in certain countries?
* Is there a bias in the dataset towards certain religions or regions? This could also be a hint at a bias in media reporting in general.

The goal of the "Ring Parable" project was to tackle these two questions in the Streamline Hackathon 2018.

This repository contains the code for the stream processing component.
The results are further visualized using a web-based UI which you can find on [Github](https://github.com/silvanheller/hackathon-scads-ui).

## Details

The structure of both the repository and the code is base on the boilerplate code provided on [Github](https://github.com/TU-Berlin-DIMA/streamline-hackathon-boilerplate).

We used the GDELT Dataset provided by the organizers of the Hackathon [1].

The stream processing part of the project was implemented using Flink and Scala.
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
