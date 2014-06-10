package com.cloudera.sa.giraph.examples;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TreeNodeVertix extends
	BasicComputation<LongWritable, Text, LongWritable,Text> {

	Pattern commonSpliter = Pattern.compile(",");
	
	@Override
	public void compute(Vertex<LongWritable, Text, LongWritable> vertex,
			Iterable<Text> messages) throws IOException {
		// TODO Auto-generated method stub
		
		TreeNodeWorkerContext workerContext = getWorkerContext();
		
		long superstep = getSuperstep();
		
		System.out.println("-id:" + vertex.getId() + " -value:" + vertex.getValue());
		System.out.println("-superstep:" + superstep);
		
		if ((superstep > 0 && workerContext.getLinksMadeThisCompute() == 0) || superstep > workerContext.getMaxSuperSteps()) {
			System.out.println("voting to halt!!");
	        vertex.voteToHalt();
	        return;
	    } else if (superstep == 0) {
			
			//In stage 0 we will send out messages to all children if we have children
			
			Text newMessage = new Text();
			
			newMessage.set(vertex.getId().toString());
			
			long messageSentCounter = 0;
			for(Edge<LongWritable, LongWritable> edge: vertex.getEdges()) {
				this.sendMessage(edge.getValue(), newMessage);
				messageSentCounter++;
				System.out.println("---SendMessage:" + edge.getValue());
			}
			if (messageSentCounter > 0) {
				this.aggregate(Const.LINKS_MADE_THIS_COMPUTE, new LongWritable(messageSentCounter));
			} else {
				this.aggregate(Const.ROOTS_COMPUTE, new LongWritable(1));
			}
			
		} else if (superstep == 1) {
			long counter = 0;
			long messageSentCounter = 0;
			//I know I could have skipped a super step here, but I'm lazy and this project is just for fun and teaching.
			for (Text message: messages) {
				counter++;
			}
			if (counter > 0) {
				System.out.println("--Is a child node");
				Text newValue = new Text(vertex.getValue().toString() + ",child");
				vertex.setValue(newValue);
			} else {
				System.out.println("--Root Sending Messages");
				Text newValue = new Text(vertex.getValue().toString() + ",root," + vertex.getId() + ",0");
				vertex.setValue(newValue);
				
				Text newMessage = new Text(vertex.getId().toString());
				for(Edge<LongWritable, LongWritable> edge: vertex.getEdges()) {
					this.sendMessage(edge.getValue(), newMessage);
					messageSentCounter++;
					System.out.println("---SendMessage:" + edge.getValue());
				}
			}
			if (messageSentCounter > 0) {
				this.aggregate(Const.LINKS_MADE_THIS_COMPUTE, new LongWritable(messageSentCounter));
			}
		} else {
			
			long messageSentCounter = 0;
			for (Text message: messages) {
				System.out.println("--GotMessage:" + message);
				String messageStr = message.toString();
				
				String[] parts = commonSpliter.split(messageStr);
				
				Text newValue = new Text(vertex.getValue().toString() + "," + messageStr + "," + (superstep-1) );
				vertex.setValue(newValue);
				
				Text newMessage = new Text(parts[0]);
				
				
				for(Edge<LongWritable, LongWritable> edge: vertex.getEdges()) {
					this.sendMessage(edge.getValue(), newMessage);
					messageSentCounter++;
					System.out.println("---SendMessage:" + edge.getValue());
				}
			}
			if (messageSentCounter > 0) {
				this.aggregate(Const.LINKS_MADE_THIS_COMPUTE, new LongWritable(messageSentCounter));
			}
		}
	}
}
