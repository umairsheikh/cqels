package org.deri.cqels.test.iswc2012;

import org.deri.cqels.engine.ExecContext;



public class StreamPlayer extends AbstractStreamPlayer {

	long throughput=1000;
	public StreamPlayer ( ExecContext context,String iri, String stFile,long throughput){
		super(context,iri,stFile);
		this.stFile=stFile;
	}
	
	
	@Override
	public void setHandler() {
		handler=new StreamElementHandler(throughput, this);	
	}

	@Override
	public long getTriples() {
		// TODO Auto-generated method stub
		return ((StreamElementHandler)handler).getTriples();
	}

}
