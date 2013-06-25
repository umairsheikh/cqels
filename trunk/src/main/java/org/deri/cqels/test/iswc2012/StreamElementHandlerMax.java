package org.deri.cqels.test.iswc2012;

import org.deri.cqels.engine.RDFStream;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;

public class StreamElementHandlerMax implements TurtleEventHandler {
	long count=0,start=System.currentTimeMillis();
	RDFStream stream;
	public StreamElementHandlerMax(RDFStream stream){
		this.stream=stream;
	}
	public void triple(int line, int col, Triple triple) {
		stream.stream(triple);
		count++; 
	}
	
	public long getTriples(){ return count;}
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
