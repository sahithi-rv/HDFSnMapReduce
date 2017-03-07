Implementation of HDFS and MapReduce

This project implements Distributed grep over a given input file using MapReduce programming paradigm.
Note: The Mapper and Reducer class are packaged in MapR.jar. MapImpl and ReduceImpl implement these classes respectively.


To run the implementation:

Protobuf:
To compile proto files run- 
	"protoc -I=. --java_out=. ./hdfs.proto"
	"protoc -I=. --java_out=. ./MapReduce.proto"

"""HDFS"""
To run Namenode: 
	java NameNodeImpl <namenode_ip> nn_port <NumDatanodes> REP

To run Datanode: 
	Note: DN_is should be equal to 'dn_port%10'
	java DataNodeImpl <DN_ip> <dn_port> <namenode_ip> <nn_port> <NumDatanodes> <DN_id>

To run HDFS client: ### This is not required to run for Distributed grep.
	java Main namenode_ip nn_port <NumDatanodes>


"""MapReduce"""

To run JobTracker:
	java JobTrackerImpl <namenode_ip> <TTs> <job_tracker_ip> <jt_port>

To run TaskTracker:
	java TaskTrackerImpl <jobtracker_ip> <tt_id> <namenode_ip>

To run JobClient:
	java JobClient MapImpl ReduceImpl <input_file in HDFS> <output_file> <numReducers> <jobtracker_ip>

Map_out_file format:
map_out_file-job_id-task_id-task_tracker_id



