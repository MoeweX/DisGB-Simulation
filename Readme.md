# DisGB Simulation

This repository contains the source code for the simulation tool from the DisGB paper (see below).
The simulation tool is used to simulate inter-broker messaging for multiple rendezvous-based distributed pub/sub strategies.
For a visualization of the broker setup from the paper, visit [https://moewex.github.io/DisGB-Simulation/](https://moewex.github.io/DisGB-Simulation/).
To better understand how brokers exchange messages for each strategy, see [Message Flows](https://github.com/MoeweX/DisGB-Simulation/blob/master/message-flows.pdf).

If you use this software in a publication, please cite it as:

### Text
Jonathan Hasenburg, David Bermbach. **DisGB: Using Geo-Context Information for Efficient Routing in Geo-Distributed Pub/Sub Systems**. In: Proceedings of the 7th IEEE/ACM International Conference on Utility and Cloud Computing 2020 (UCC 2020). IEEE 2020.

### BibTeX
```
@inproceedings{hasenburg_disgb:_2020,
	title = {{DisGB}: Using Geo-Context Information for Efficient Routing in Geo-Distributed Pub/Sub Systems},
	booktitle = {2020 {IEEE}/{ACM} International Conference on Utility and Cloud Computing},
	publisher = {{IEEE}},
	author = {Hasenburg, Jonathan and Bermbach, David},
	year = {2020}
}
```

## How to build manually?

- Run `mvn install` in the root of the [GeoBroker](http://www.github.com/moewex/geobroker) project, since it is a dependency.
- Run `mvn package` in the root directory of this project

## How to run locally?

Preparation
- Install Java 11
- Build project or use pre-built Jar found at `./Broker-Simulation.jar`
- Get the [worldcities.csv](https://simplemaps.com/data/world-cities) data and store it at `./worldcities.csv`.


To run the experiment:
```bash
mkdir tmp-results
java -jar -Xmx4G Broker-Simulation.jar -d tmp-results/ -i worldcities.csv -p exp --nBrokers 9 --nClients 1000 --nTopics 10 --pop 1000000 -e 900 --eGeofence 5.0 --sGeofence 10.0 --fieldSize 6 --history false
```
To learn more about command line argument options, run `java -jar Broker-Simulation.jar -h`.

## How to visualize broker locations and strategies on a globe?

First, get results using the main Kotlin project.
The results files need to be renamed for the visualization using the included script. Delete the sample files, copy all results into
`docs/CSV` and execute `./docs/rename.sh`.
Now everything is in place. However, a web server is needed for the broswer visualization to work (security restrictions of browsers).
To begin the visualization, follow these steps (if Python 3 is available):

- Navigate to `docs`
- Run `python3 -m http.server 8080`
- Open a web browser and navigate to [http://localhost:8080/index.html](http://localhost:8080/index.html).

Some notes:
- If you change the CSV file, beware browser caching! Changes might not display because of it.
- The loaded CSV file address is hardcoded. To change the name and port you have to change it in the code (when 8080 is unavailable).
- Also, depending on how many brokers the experiment includes, the visualization might not have a unique color for all of them. The colors repeat after they are exhausted. The color map includes 28 colors.

Further infos are inside of the `README.md` inside `docs`.

## Simplifications:

- For all strategies, locations are flooded to all brokers. Thus, counting and comparing location updates does not make
 sense.
- While clients can be mobile, it is expected that they do not move out of the broker area of their original local
 broker.
- The unique identifier of a message is the origin clientId and tick
