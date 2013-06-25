package org.deri.cqels.test.iswc2012;

import org.deri.cqels.engine.ExecContext;

public class StreamPlayerMax extends AbstractStreamPlayer {
	public StreamPlayerMax ( ExecContext context,String iri, String stFile){
		super(context,iri,stFile);
	}
	

	@Override
	public void setHandler() {
		// TODO Auto-generated method stub
		 handler=new StreamElementHandlerMax(this);
	}

	@Override
	public long getTriples() {
		// TODO Auto-generated method stub
		return ((StreamElementHandlerMax)handler).getTriples();
	}

}
