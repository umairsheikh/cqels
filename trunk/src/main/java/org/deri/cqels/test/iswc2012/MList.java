package org.deri.cqels.test.iswc2012;

import java.io.PrintStream;
import java.util.Iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.sparql.core.Var;

public class MList implements ContinuousListener{
		public static long count=0;
		PrintStream out;
		ExecContext context;
	    public MList(PrintStream out,ExecContext context){
	    	this.out=out;
	    	this.context=context;
	    }
		public void update(Mapping mapping) {
			String result="";
			for(Iterator<Var> vars=mapping.vars();vars.hasNext();)
				result+=" "+context.engine().decode(mapping.get(vars.next()));
			out.println(result);
			count++;
		}
		public void decode(){};
		public void printf(){};
	
}
