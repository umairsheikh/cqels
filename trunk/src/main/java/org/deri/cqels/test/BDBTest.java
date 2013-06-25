package org.deri.cqels.test;

import java.io.File;
import java.util.Map.Entry;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.bind.tuple.TupleTupleKeyCreator;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;

public class BDBTest {
	public static void main( String[] args )
    {
		EnvironmentConfig config= new EnvironmentConfig();
		config.setAllowCreate(true);
	
		Environment env= new Environment(new File("/home/chanlevan/workspace/CQELS/cache"), config);
		
		DatabaseConfig dbConfig=new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		dbConfig.setTemporary(true);
		
		Database db= env.openDatabase(null, "samplecache", dbConfig);
		
		SecondaryConfig idxConfig= new SecondaryConfig();
		idxConfig.setAllowCreate(true);
		idxConfig.setSortedDuplicates(true);
		idxConfig.setKeyCreator(new TupleTupleKeyCreator<Entry<Long, Long>>(){

			@Override
			public boolean createSecondaryKey(TupleInput primaryKeyInput,
					TupleInput dataInput, TupleOutput indexKeyOutput) {
				indexKeyOutput.write(dataInput.getBufferBytes());
				return true;
			}
		});
		
		SecondaryConfig idx2Config= new SecondaryConfig();
		idx2Config.setAllowCreate(true);
		idx2Config.setSortedDuplicates(true);
		idx2Config.setKeyCreator(new TupleTupleKeyCreator<Entry<Long, Long>>() {

			@Override
			public boolean createSecondaryKey(TupleInput primaryKeyInput,
					TupleInput dataInput, TupleOutput indexKeyOutput) {
				
				long tmp=dataInput.readLong();
				indexKeyOutput.writeLong(dataInput.readLong());
				indexKeyOutput.writeLong(tmp);
				return true;
			}
		});
		SecondaryDatabase idx= env.openSecondaryDatabase(null, "cacheidx1", db, idxConfig);
		SecondaryDatabase idx2= env.openSecondaryDatabase(null, "cacheidx2", db, idx2Config);
		
		long start=System.currentTimeMillis();
		long last=0;
		for(int i=0;i<1E6;i++){
			DatabaseEntry key= new DatabaseEntry();
			Long time=System.nanoTime();
			LongBinding.longToEntry(time, key);
			TupleOutput data= new TupleOutput();
			data.writeLong(time+1);
			data.writeLong(time+2);
			db.put(null, key, new DatabaseEntry(data.toByteArray()));
			
			if(i>1E5){
				LongBinding.longToEntry(last, key);
				db.delete(null, key);
			}
			last=time;
		}
		long el=(System.currentTimeMillis()-start);
		System.out.println("elapsed time"+ el + " numf of record per milisec " +1E6/el);
    }
}
