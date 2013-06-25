package org.deri.cqels.test;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;

import com.hp.hpl.jena.sparql.core.Var;


public class StreamListener implements ContinuousListener {
	public int count= 0;
	public void update(Mapping mapping){
		count++;
		if(count%1E2==0){
			System.out.println(count + " at "+System.currentTimeMillis());
			System.out.println(mapping.getCtx().engine().decode(mapping.get(Var.alloc("person1")))+" -> " +
					mapping.getCtx().engine().decode(mapping.get(Var.alloc("person2"))));
		}
	}
	public void decode(){};
	public void printf(){};
}
