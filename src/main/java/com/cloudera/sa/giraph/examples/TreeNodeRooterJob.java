package com.cloudera.sa.giraph.examples;

import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TreeNodeRooterJob 
{
	public static void main(String[] args) throws Exception {
	    
		if (args.length != 3) {
			System.out.println("TreeNodeRooterJob Help:");
			System.out.println("Parameters: TreeNodeRooter <numbersOfWorkers> <inputLocaiton> <outputLocation>");
			System.out.println("Example:TreeNodeRooter 2 inputFolder outputFolder");
			return;
		}
		
		JobContext j;
		
		String numberOfWorkers = args[0];
		String inputLocation = args[1];
		String outputLocation = args[2];
		
	    GiraphJob bspJob = new GiraphJob(new Configuration(), TreeNodeRooterJob.class.getName());
	    
	    bspJob.getConfiguration().setVertexClass(TreeNodeVertix.class);
	    bspJob.getConfiguration().setVertexInputFormatClass(TreeRecordTextVertexInputFormat.class);
	    GiraphFileInputFormat.addVertexInputPath(bspJob.getConfiguration(), new Path(inputLocation));
	    
	    bspJob.getConfiguration().setVertexOutputFormatClass(TreeRecordTextVertexOutputFormat.class);
	    bspJob.getConfiguration().setWorkerContextClass(TreeNodeWorkerContext.class);
	    bspJob.getConfiguration().setMasterComputeClass(TreeNodeVertixMasterCompute.class);
	    
	    int minWorkers = Integer.parseInt(numberOfWorkers);
	    int maxWorkers = Integer.parseInt(numberOfWorkers);
	    bspJob.getConfiguration().setWorkerConfiguration(minWorkers, maxWorkers, 100.0f);

	    FileOutputFormat.setOutputPath(bspJob.getInternalJob(),
	                                   new Path(outputLocation));
	    boolean verbose = true;
	    
	    if (bspJob.run(verbose)) {
	      System.out.println("Ended Good");
	    } else {
	      System.out.println("Ended with Failure");
	    }

	}
	 
}
