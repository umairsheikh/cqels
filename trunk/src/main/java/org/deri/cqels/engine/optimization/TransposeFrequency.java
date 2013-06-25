package org.deri.cqels.engine.optimization;

import java.util.Set;

public class TransposeFrequency implements FrequencyMatrix {
	FrequencyMatrix base;
	public TransposeFrequency(FrequencyMatrix base){
		this.base=base;
	}
	public void add(long i, long j, int value) {
		base.add(j,i, value);
	}

	public void minus(long i, long j, int value) {
		base.minus(j, i, value);
	}

	public int freq(long i, long j) {
		return base.freq(j,i);
	}
	public Set<Long> rows() {
		// TODO Auto-generated method stub
		return base.columns();
	}
	public Set<Long> columns() {
		// TODO Auto-generated method stub
		return base.rows();
	}

}
