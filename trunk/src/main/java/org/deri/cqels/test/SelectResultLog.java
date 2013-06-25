package org.deri.cqels.test;

import java.util.Iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.sparql.core.Var;

public class SelectResultLog implements ContinuousListener {
	public static long end = 0, count = 0;
	ExecContext context;
	public SelectResultLog(ExecContext context) {
		this.context = context;
	}
	public void update(Mapping mapping) {
		//if(count%1E0==0) System.out.println("result "+mapping);
		for (Iterator<Var> vars = mapping.vars(); vars.hasNext(); ) {
			Var var = vars.next();
			System.out.print(this.context.engine().decode(mapping.get(var)) + " ");
		}
		System.out.println();
		count ++;
		end=System.nanoTime();
	}
	
	public static void reset(){ count=0;}
	public void decode(){};
	public void printf(){};
}
