---
title: CSCI E-88 Final Project
author: David Shaub
geometry: margin=2cm
date: 2018-05-08
---

# Project Goal

# YouTube Demo
TODO: Insert link here

# Big Data Source

# Expected Results

# Processing Pipeline

![](architecture.png)


Everything needed to run the project is contained inside `transaction_tracker.tar.gz`. The only requirements are Docker, an active internet connection to download the packages for the Docker container and to connect to the Blockchain.info API, and a browser to view the Grafana UI.

# Results

## Build the project

From the top-level directory with `Dockerfile`, launch:
```
$ docker build . --compress -t transaction_tracker:1.0
```

## Launch the container

Once the build success, run the container:
```
$ docker run --init -p 3000:3000 -p 8086:8086 -ti transaction_tracker:1.0 /root/transaction_tracker/run_pipeline.sh
```

You should start to see output like
```
Starting influxdb...
influxdb process was started [ OK ]
 * Starting Grafana Server                                                                                                                                                                           [ OK ] 
Using api key: <REDACTED>
Writing 195 transactions
Using api key: <REDACTED>
Processing new block at height 519900
Processing new block at height 519890
Processing new block at height 519891
Processing new block at height 519892
Processing new block at height 519893
Processing new block at height 519894
Processing new block at height 519895
Processing new block at height 519896
Writing 160 transactions
Processing new block at height 519897
Processing new block at height 519898
Processing new block at height 519899
Writing 167 transactions
Writing 187 transactions
Writing 165 transactions
Writing 175 transactions
Launching batch job at 2018-04-25T192329Z
Launching Beam job at 2018-04-25T192329Z
Launching InfluxDB insert at 2018-04-25T192329Z
Writing 150 transactions
```

The pipeline will start collecting data and running. Results will appear in the Grafana dashboard in ~10 minutes, so go ahead and open up and start configuring Grafana.


## Using Grafana

![](grafana_dashboard.png)

Access the Grafana dashboard at `localhost:3000`. The default credentials as `admin` and `admin`.
Add a data source named `unconfirmed` using `InfluxDB` as the type. Use URL `http://localhost:8086` with `proxy` access. The database name is `blockchain`, and the Min time intervals should be `5m`.

Create your own visualizations for the plotting unconfirmed transaction volume across time. Alternatively, import `grafana_dashboard.json` to get started. The Grafana documentation claims that the `.json` file _should_ contain all of the 


## Troubleshooting

Even though an API key is used to attempt to avoid API rate-limiting, timeout errors can occur. If necessary, tweak `SLEEP_TIME` in `fetch_mempool.py`. The default value of 2 should provide a reasonable balance. Sleep times as low as 1 were tested and proved stable in some circumstances but failed in others. Running this with a high sleep values (in particular less than once every 10 seconds) will result in missed transactions, however, so we desire to keep this sleep time as low as possible. If there are failures, simply exit the Docker container, rebuild with a longer `SLEEP_TIME`, and launch the container again.

Both Grafana and InfluxDB have their ports mapped outside the container, so you can launch `influx` on your host OS and access data inside InfluxDB to see the data that is stored.
```
$ influx
Connected to http://localhost:8086 version 1.5.2
InfluxDB shell version: v1.5.2
> use blockchain;
Using database blockchain
> show measurements;
name: measurements
name
----
unconfirmed_transactions
> select * from unconfirmed_transactions;
name: unconfirmed_transactions
time                count
----                -----
1524684209000000000 1017
```

# Conclusions and Lesson Learned
* Like Spark, Apache Beam uses lazy evaluation and does not perform calculation on results until the final result must be computed. Unfortunately, beam does not have a REPL for examining intermediate data. This means debugging required writing out complete jobs that write to output files and slowly and incrementally adding on functionality as in a compiled language vs development with an interpreted language. However, PySpark makes examining intermediate results for debugging very easy, so although the Apache Beam model seeks to unite the streaming/batch APIs, it seems somewhat more difficult to develop for compared to Spark's established toolset.
* Two major limitations exist in this pipeline and technologies: the query frequency of the public API and Apache Beam's IO format support. The Blockchain.info API was on some occassions stable when queried once per second, but on other occassions timeout errors occured. Beam's limited Python support meant that reading and writing from filesystem was necessary instead of directly writing to a database or messaging service as would be ideal.
* If implementing this project a second time, more care would be taken on the collection tier for ways to make collection robust. In particular, the current collection tier code performs more than simple collection: right now it also performs deduplication to avoid writing redundant data that Apache Beam will remove anyway. Although the data volume for a single thread here is reasonable (and in fact bounded by the Bitcoin network to be approximately 200KB/minute), we ideally would record all data we received and then remove duplicates in a single location later. If we were collecting data from our own node instead of from Blockchain.info, this problem could also go away, and our collection tier would be a very simple `tail -f` piped to `grep` on the node logs.
* Since the Blockchain.info API was the main source of job failure and running your own network node takes several weeks to sync with the chain, the natural improvement with more time would be to start your own node for collection instead.
* The batch job that runs in Apache Beam would be a good location for alternative technologies. If we desire to operate this with streaming support, we could use Spark, Flink, or Storm here. These also have better IO support with connectors to additional databases, so these would have been good choices in the pipeline.
* The obvious enhancement to this project would be to utilize your own `bitcoind` node instead of relying on the Blockchain.info APIs. By tailing the logs from our own node, we could guarantee that we do not miss events. Furthermore, we could set our own mempool parameters and run several nodes with very permissive retention policies so as to collect transactions that Blockchain.info may never have received. Additionally, if we wish to run this project for many months instead of merely for a few hours, we should lower the batch frequency to an acceptable level (e.g. once per day) since the amount of data will continue to grow as more transactions occur.
