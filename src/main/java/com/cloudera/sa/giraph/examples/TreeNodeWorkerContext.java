package com.cloudera.sa.giraph.examples;

import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.LongWritable;

public class TreeNodeWorkerContext extends WorkerContext {

	long lastLinksMade = 0;
	
	/** Number of supersteps to run (6 by default) */
    private int maxSupersteps = 6;
    private long linksMadeThisCompute = 0;
    
    public static final String SUPERSTEP_COUNT =
    	      "treenode.superstepCount";
	
	@Override
	public void preApplication() throws InstantiationException,
			IllegalAccessException {
		maxSupersteps = getContext().getConfiguration()
		          .getInt(SUPERSTEP_COUNT, maxSupersteps);
	}

	@Override
	public void postApplication() {
		
	}

	@Override
	public void preSuperstep() {
		System.out.println("PreSuperStep:" + ((LongWritable)getAggregatedValue(Const.LINKS_MADE_THIS_COMPUTE)).get());
		linksMadeThisCompute = ((LongWritable)getAggregatedValue(Const.LINKS_MADE_THIS_COMPUTE)).get();
		
	}

	@Override
	public void postSuperstep() {
		System.out.println("PostSuperStep:" + ((LongWritable)getAggregatedValue(Const.LINKS_MADE_THIS_COMPUTE)).get());
		lastLinksMade = linksMadeThisCompute;
	}
	
	public int getMaxSuperSteps() {
		return maxSupersteps;
	}
	
	public long getLinksMadeThisCompute() {
		return linksMadeThisCompute;	
	}
	
	public long getLastLinksMade() {
		return lastLinksMade;
	}
	


}
