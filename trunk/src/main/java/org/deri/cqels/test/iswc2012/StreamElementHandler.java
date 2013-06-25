package org.deri.cqels.test.iswc2012;

import org.deri.cqels.engine.RDFStream;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;

public class StreamElementHandler implements TurtleEventHandler {
	long count=0,start=System.currentTimeMillis(),throughput=10000,count2=0;
	RDFStream stream;
	public StreamElementHandler(long throughput,RDFStream stream){
		this.throughput=throughput;
		this.stream=stream;
	}
	public void triple(int line, int col, Triple triple) {
		// TODO Auto-generated method stub
		//System.out.println(triple+" at "+line);
		stream.stream(triple);
		count++; 
		count2++;
		if(count==(throughput/10)){
			
			long sleep=100-(System.currentTimeMillis()-start);
			if(sleep>0){
				try {
					//System.out.println("sleep "+sleep);
					Thread.sleep(sleep);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			count=0;
			start=System.currentTimeMillis();
		}
	}
	
	public long getTriples(){ return count2;}
	
	public void prefix(int line, int col, String prefix, String iri) {
		// TODO Auto-generated method stub

	}

	public void startFormula(int line, int col) {
		// TODO Auto-generated method stub

	}

	public void endFormula(int line, int col) {
		// TODO Auto-generated method stub

	}

}
