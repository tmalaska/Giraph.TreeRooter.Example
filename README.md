# Giraph.TreeRooter.Example
## Generate Overview
This is a very simple Giraph job for the main purpose of teaching.

The example will take nodes from a text file in the following format

{ID}|{Name}|{IDs of child separated by commons}

Examples of this format can be found in the example dataset in the sampledata folder

The final output of this job will be

{ID}|{Name},{Root or child},{True Root ID},{Depth in Tree}|{IDs of child separated by commons}

This has many uses, in that it shows that Giraph is a great tool for building trees.

##Setup Giraph
At the time of making this Github Giraph doesn't have a version on public maven repository.  So this is what you do to set up a local repository.

Download version 1.0.0 : http://www.apache.org/dyn/closer.cgi/giraph/giraph-1.0.0

Apply the following patch so it will work good on CDH : http://www.mail-archive.com/user@giraph.apache.org/msg00945/check.diff

This use mvn to build and install with the following command: 
mvn install -Phadoop_cdh4.1.2 -DskipTests

##Build This Example
Just use the following maven command
mvn clean package

In the target folder there should be a ruinable jar.

to execute use the hadoop cmd like the following

hadoop jar TreeNodeRooterJob.job {numbersOfWorkers} {inputLocaiton} {outputLocation}

or

hadoop jar TreeNodeRooterJob.job 2 input/dataset.1.txt output 

