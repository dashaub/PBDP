
## Problem 1

On OS X we can easily install these with `brew`:
```
$ brew install elasticsearch kibana
$ elasticsearch --version
Java HotSpot(TM) 64-Bit Server VM warning: Cannot open file logs/gc.log due to No such file or directory

Version: 6.2.4, Build: ccec39f/2018-04-12T20:37:28.497551Z, JVM: 1.8.0_111
$ kibana --version
6.2.4
```

We download and run Elasticsearch and Kibana
```
# Download and extract Elasticsearch
$ wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.2.4.tar.gz
$ tar xf elasticsearch-6.2.4.tar.gz 
# Download and extract Kibana for OS X
$ wget https://artifacts.elastic.co/downloads/kibana/kibana-6.2.4-darwin-x86_64.tar.gz
$ tar xf kibana-6.2.4-darwin-x86_64.tar.gz
# Install X-pack
$ elasticsearch-6.2.4/bin/elasticsearch-plugin install x-pack
# Disable security
$ echo 'xpack.security.enabled: false' >> elasticsearch-6.2.4/config/elasticsearch.yml 
$ echo 'xpack.security.enabled: false' >> kibana-6.2.4-darwin-x86_64/config/kibana.yml 
# Start Elasticsearch and Kibana (ideally do this in two separate terminals so we can see logs)
$ elasticsearch-6.2.4/bin/elasticsearch
$ kibana-6.2.4-darwin-x86_64/bin/kibana
```

![monitoring Elasticsearch]()
