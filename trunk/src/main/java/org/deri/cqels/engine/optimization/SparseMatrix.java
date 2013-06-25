package org.deri.cqels.engine.optimization;

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.openjena.atlas.lib.Pair;


public class SparseMatrix implements FrequencyMatrix {
	TreeSet<Long> r, c;
	IdentityHashMap<Pair<Long,Long>, Integer> freq;
	public SparseMatrix(){
		
		r= new TreeSet<Long>(); c=new TreeSet<Long>();
		freq= new IdentityHashMap<Pair<Long,Long>, Integer>();
	}
	
	/* (non-Javadoc)
	 * @see org.deri.cqels.engine.optimization.FreqencyM#add(long, long, int)
	 */
	
	public void add(long i,long j, int value){
		r.add(i); c.add(j);
		freq.put(new Pair<Long,Long>(i,j), value+freq(i, j));
	}
	
	/* (non-Javadoc)
	 * @see org.deri.cqels.engine.optimization.FreqencyM#remove(long, long, int)
	 */
	
	public void minus(long i,long j,int value){
		freq.put(new Pair<Long,Long>(i,j), freq(i, j)-value);
	}
	
	/* (non-Javadoc)
	 * @see org.deri.cqels.engine.optimization.FreqencyM#freq(long, long)
	 */
	
	public int freq(long i, long j){
		Integer f= freq.get(new Pair<Long, Long>(i, j));
		if(f!=null) return f;
		return 0;
	}

	public Set<Long> rows() {
		// TODO Auto-generated method stub
		return r;
	}

	public Set<Long> columns() {
		// TODO Auto-generated method stub
		return c;
	}
}
