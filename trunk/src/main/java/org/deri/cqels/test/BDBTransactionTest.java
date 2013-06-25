package org.deri.cqels.test;

import java.io.File;
import java.io.UnsupportedEncodingException;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.Transaction;

public class BDBTransactionTest {

	/**
	 * @param args
	 * @throws UnsupportedEncodingException 
	 */
	public static void main(String[] args) throws UnsupportedEncodingException {
		// TODO Auto-generated method stub
		EnvironmentConfig config= new EnvironmentConfig();
		config.setAllowCreate(true);
		
		//config.setTransactional(true);
		Environment env= new Environment(new File("/test/cache/"), config);
		test(env);
	}
	
	public static void test(Environment env) throws UnsupportedEncodingException{
		DatabaseConfig dbConfig=new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		Database buff=env.openDatabase(null, "pri_synopsis_",dbConfig);
		
	    String dataString = "thedata";
	    DatabaseEntry key = new DatabaseEntry();
	    
	    for(int i=0;i<10;i++){
	    	long time=System.nanoTime();
	    System.out.println(time);
	    	LongBinding.longToEntry(time, key);
	    DatabaseEntry data = 
	        new DatabaseEntry(new byte[0]);
			buff.put(null, key, data);
	    }
	    Cursor cursor= buff.openCursor(null, CursorConfig.READ_COMMITTED);
	    DatabaseEntry data = new DatabaseEntry();
	    cursor.getNext(key, data, LockMode.DEFAULT);
	    System.out.println(" t=" +LongBinding.entryToLong(key));
	    cursor.delete();
	    cursor.getNext(key, data, LockMode.DEFAULT);

	    System.out.println(" t=" +LongBinding.entryToLong(key));
	}

}
