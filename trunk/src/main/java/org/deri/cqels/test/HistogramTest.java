package org.deri.cqels.test;

import org.apache.commons.math.MathException;
import org.apache.commons.math.stat.inference.ChiSquareTest;
import org.apache.commons.math.stat.inference.ChiSquareTestImpl;

public class HistogramTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ChiSquareTestImpl ch= new ChiSquareTestImpl();
		long[] o={10,6,4,5,16};
		double []e={4.5,2.3,1.2,2.5,5.3};
		try {
			System.out.println(ch.chiSquareTest(e,o));
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (MathException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

}
