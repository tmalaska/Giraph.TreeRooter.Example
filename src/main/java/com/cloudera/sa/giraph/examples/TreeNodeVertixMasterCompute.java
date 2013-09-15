package com.cloudera.sa.giraph.examples;


import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;

public class TreeNodeVertixMasterCompute extends DefaultMasterCompute {
	
	
	
	@Override
    public void compute() {
		
		LongWritable linksMade = getAggregatedValue(Const.LINKS_MADE_THIS_COMPUTE);
		
		System.out.println("linksMade:" + linksMade);
		System.out.println("roots:" + getAggregatedValue(Const.ROOTS_COMPUTE));
		System.out.println("getTotalNumEdges():" + getTotalNumEdges());
		System.out.println("getTotalNumVertices():" + getTotalNumVertices());
		
		/*
		if (getSuperstep() == 10) {
			System.out.println("Hit ten super steps.");
			haltComputation();
		} else if (getSuperstep() > 1 && linksMade.get() == 0) {
			System.out.println("No more children made in the last superstep.");
			haltComputation();
		
		}
		*/
		
		
    }

    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
    	registerAggregator(Const.LINKS_MADE_THIS_COMPUTE,
    	          LongSumAggregator.class);
    	
    	registerAggregator(Const.ROOTS_COMPUTE,
  	          LongSumAggregator.class);
    }
}
