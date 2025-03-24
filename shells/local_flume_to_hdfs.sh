# example.conf: A single-node Flume configuration

# Name the components on this agent
a1.sources = r1
a1.sinks = k1
a1.channels = c1

# Describe/configure the source

a1.sources.r1.type = TAILDIR
a1.sources.r1.channels = c1
a1.sources.r1.positionFile = /var/log/flume/taildir_position.json
a1.sources.r1.filegroups = f1
a1.sources.r1.filegroups.f1 = /opt/2207A/runbo_zhang/data_mock/gmall/log/app.2025-03-23.log
a1.sources.r1.headers.f1.headerKey1 = value1
a1.sources.r1.fileHeader = true
a1.sources.ri.maxBatchCount = 1000

# Describe the sink

a1.sinks.k1.type = hdfs

a1.sinks.k1.hdfs.path = /2207A/runbo_zhang/gmall/log/2025-03-23/
a1.sinks.k1.hdfs.filePrefix = events-
a1.sinks.k1.hdfs.round = false
a1.sinks.k1.hdfs.rollSize = 102400
a1.sinks.k1.hdfs.rollCount = 0
a1.sinks.k1.hdfs.fileType = DataStream



# Use a channel which buffers events in memory
a1.channels.c1.type = memory
a1.channels.c1.capacity = 1000
a1.channels.c1.transactionCapacity = 100

# Bind the source and sink to the channel
a1.sources.r1.channels = c1
a1.sinks.k1.channel = c1



bin/flume-ng agent --conf conf --conf-file local_flume_to_hdfs.conf --name a1 -Dflume.root.logger=INFO,console
