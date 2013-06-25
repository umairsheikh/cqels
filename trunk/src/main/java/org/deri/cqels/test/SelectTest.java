package org.deri.cqels.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;

import org.codehaus.stax2.validation.Validatable;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.IndexedTripleRouter;
import org.deri.cqels.engine.ContinuousListener;

import com.espertech.esper.collection.Pair;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.FileManager;

public class SelectTest {

	/**
	 * @param args
	 */
	static Node rfid=Node.createURI("http://deri.org/streams/rfid");
	static String CQELSHOME,RUNMODE,query,DATASIZE,CQELSDATA,log;
	static  int STREAMSIZE=(int)5E3,noQ=1,step=(int)5E3;
	public static void main(String[] args) {
		
		if(args.length<7) System.exit(-1);
		
		//System.out.println(args.length);
		
		// TODO Auto-generated method stub
		RUNMODE=args[0];
		CQELSDATA=args[1];
		CQELSHOME=args[2];
		query=args[3];
		DATASIZE=args[4];
		STREAMSIZE=Integer.parseInt(args[5]);
		
		boolean create=Boolean.valueOf(args[6]);
		
		if((args.length>7)&&(!"query2".equalsIgnoreCase(query)))
			noQ=Integer.parseInt(args[7]);
		
		FileManager filemanager = FileManager.get() ;
        String queryString = filemanager.readWholeFileAsUTF8(CQELSDATA+"/"+query+".cqels") ;
        
        final ExecContext context=new ExecContext(CQELSHOME, create);
        if(create){
        	context.loadDefaultDataset(CQELSDATA+"/"+DATASIZE+".rdf");
            context.loadDataset("http://deri.org/floorplan/", CQELSDATA+"/floorplan.rdf");
        }
        
        
        int j=0;
        Pair<Long, Long> pair=new Pair<Long, Long>((long)0, (long)0);
		BufferedReader reader;

		ContinuousListener sl = new SelectResultLog(context);
		
		try {
			reader = new BufferedReader(new FileReader(CQELSDATA+"/authors.text"));
			String name;
			while ((name = reader.readLine()) != null &&j++<noQ)   {
				if( RUNMODE.equalsIgnoreCase("single")&&(j<noQ)) continue;
				ContinuousSelect cq=context.registerSelect(query(queryString, name));
		        cq.register(sl);
			      
				
		      //System.out.println(cq.getOp()+"");
		      
		        /*if(RUNMODE.equalsIgnoreCase("single")){
		        	
		        	Pair<Long, Long> report=stream(context);
		        	pair.setFirst(pair.getFirst()+report.getFirst());
		        	pair.setSecond(pair.getSecond()+report.getSecond());
		        	ResultLog.reset();
		        }*/
			 }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		if(RUNMODE.equalsIgnoreCase("single")){
			if(query.equalsIgnoreCase("query5")) step=500;
			log=CQELSHOME+"/results/"+query+"_"+DATASIZE+"_"+STREAMSIZE+"_single_"+noQ+".log";
        }else if(RUNMODE.equalsIgnoreCase("multiple")){
        	 step=(step/noQ)*10;
        	 log=CQELSHOME+"/results/"+query+"_"+DATASIZE+"_"+STREAMSIZE+"_multilple_"+noQ+".log";
		}
		
		report(stream(context));
		System.out.print("match value:" + SelectResultLog.count);
	}
	
	public static void report(Pair<Long,Long> report){
		 FileWriter writer;
		try {
			writer = new FileWriter(log);
			writer.write((float)report.getFirst()/(float)(STREAMSIZE*(1E6))+"\n");
			writer.write("\n");
			writer.write(""+report.getSecond());
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static String query(String template,String name){
		//System.out.println(template.replaceAll("AUTHORNAME", name));
		return template.replaceAll("AUTHORNAME", name);
	}
	
	public static Pair<Long, Long> stream(ExecContext context) {
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(CQELSDATA+"/stream/rfid_50000.stream"));
		
			String strLine;
			 long start=System.nanoTime();
			 int i=0;
			 System.out.println("start "+query +" at " +noQ);
			 int count = 0; 
			while ((strLine = reader.readLine()) != null &&i<STREAMSIZE)   {
				if(i%step==0) System.out.println("line "+i+" "+strLine +" "+System.currentTimeMillis());
			    String[] data=strLine.split(" ");
				context.engine().send(rfid,n(data[0]),n(data[1]),n(data[2]));	
				
				i++;
				if (strLine.contains("Paul_Erdoes")) count ++;
			 }
			
			 System.out.println("count: " + count);
			 long end=System.nanoTime();
			 try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			 long el=Math.max(end, SelectResultLog.end)-start;
			
			 System.out.println(query +" stop after " +el +" nano seconds and "+IndexedTripleRouter.accT +" nanoseconds for deleting");
			
			 return new Pair<Long, Long>(el, SelectResultLog.count);
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Pair<Long, Long>((long)0, (long)0);
	}
	
	public static  Node n(String st){
		return Node.createURI(st);
	}
}
