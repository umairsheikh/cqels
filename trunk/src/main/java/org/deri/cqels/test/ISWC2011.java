package org.deri.cqels.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.IndexedTripleRouter;

import com.espertech.esper.collection.Pair;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.util.FileManager;

public class ISWC2011 
{
	static Node rfid = Node.createURI("http://deri.org/streams/rfid");
	static String CQELSHOME, RUNMODE, query, DATASIZE, CQELSDATA, log;
	static  int STREAMSIZE = (int)5E3, noQ = 1,step = (int)5E3;
	
	public static void main(String[] args) {
		if(args.length < 7) {
			System.exit(-1);
		}
		
		RUNMODE = args[0];
		CQELSDATA = args[1];
		CQELSHOME = args[2];
		query = args[3];
		DATASIZE = args[4];
		STREAMSIZE = Integer.parseInt(args[5]);
		
		boolean create = Boolean.valueOf(args[6]);
		
		if((args.length > 7)&&(!"query2".equalsIgnoreCase(query)))
			noQ = Integer.parseInt(args[7]);
		
		FileManager filemanager = FileManager.get();
        String queryString = filemanager.readWholeFileAsUTF8(CQELSDATA+"/"+query+".cqels") ;
        
        ExecContext context = new ExecContext(CQELSHOME, create);
        if(create) {
        	context.loadDefaultDataset(CQELSDATA + "/" + DATASIZE + ".rdf");
            context.loadDataset("http://deri.org/floorplan/", CQELSDATA+"/floorplan.rdf");
        }
                
        int j=0;
		BufferedReader reader;
		String name = null;
		try {
			reader = new BufferedReader(new FileReader(CQELSDATA + "/authors.text"));
			while (j++ < noQ && (name = reader.readLine()) != null ) {
				if( RUNMODE.equalsIgnoreCase("single")&&(j<noQ)) continue;
				ContinuousSelect cq = context.registerSelect(generateQuery(queryString, name));
		        cq.register(new SelectResultLog(context));
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
        
		if(RUNMODE.equalsIgnoreCase("single")) {
			if(query.equalsIgnoreCase("query5")) 
				step=500;
			log=CQELSHOME+"/results/"+query+"_"+DATASIZE+"_"+STREAMSIZE+"_single_"+noQ+".log";
        }
		else if(RUNMODE.equalsIgnoreCase("multiple")) {
        	 step=(step/noQ)*10;
        	 log=CQELSHOME+"/results/"+query+"_"+DATASIZE+"_"+STREAMSIZE+"_multilple_"+noQ+".log";
		}
		
		report(stream(context));
		System.out.println("matched value: " + SelectResultLog.count );
	}
	
	private static void report(Pair<Long,Long> report)
	{
		 FileWriter writer;
		try 
		{
			writer = new FileWriter(log);
			writer.write((float)report.getFirst()/(float)(STREAMSIZE*(1E6))+"\n");
			writer.write("\n");
			writer.write(""+report.getSecond());
			writer.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	private static String generateQuery(String template,String name)
	{
		return template.replaceAll("AUTHORNAME", name);
	}
	
	private static Pair<Long, Long> stream(ExecContext context) 
	{
		try 
		{
			int count = 0;
			BufferedReader reader = new BufferedReader(new FileReader(CQELSDATA + "/stream/rfid_50000.stream"));
		
			String strLine;
			long start = System.nanoTime();
			int i = 0;
			System.out.println("start " + query + " at " +noQ);
			while ((strLine = reader.readLine()) != null && i < STREAMSIZE) {
				if(i % step == 0) 
					System.out.println("line " + i + " "+ strLine + " " + System.currentTimeMillis());
			    String[] data = strLine.split(" ");
				context.engine().send(rfid,n(data[0]),n(data[1]),n(data[2]));	
				i++;
			 }
			 reader.close();
			 System.out.println("The number of matched value: " + count);
			 long end=System.nanoTime();
			 try 
			 {
				Thread.sleep(5000);
			 } 
			 catch (InterruptedException e) 
			 {
				// TODO Auto-generated catch block
				e.printStackTrace();
			 }
			
			 long el=Math.max(end, SelectResultLog.end)-start;
			
			 System.out.println(query +" stop after " +el +" nano seconds and "+IndexedTripleRouter.accT +" nanoseconds for deleting");
			
			 return new Pair<Long, Long>(el, SelectResultLog.count);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
		
		return new Pair<Long, Long>((long)0, (long)0);
	}
	
	public static  Node n(String st)
	{
		return Node.createURI(st);
	}
}
