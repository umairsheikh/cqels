package org.deri.cqels.engine.optimization;

import java.util.Set;

public interface FrequencyMatrix {
	/* (non-Javadoc)
	 * @see org.deri.cqels.engine.optimization.FreqencyM#add(long, long, int)
	 */
	public void add(long i, long j, int value);

	/* (non-Javadoc)
	 * @see org.deri.cqels.engine.optimization.FreqencyM#remove(long, long, int)
	 */
	public void minus(long i, long j, int value);

	public Set<Long> rows();
	public Set<Long> columns();
	/* (non-Javadoc)
	 * @see org.deri.cqels.engine.optimization.FreqencyM#freq(long, long)
	 */
	public int freq(long i, long j);
}
