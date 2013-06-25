package org.deri.cqels.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.deri.cqels.engine.ContinuousConstruct;
import org.deri.cqels.engine.ConstructListener;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.IndexedTripleRouter;

import com.espertech.esper.collection.Pair;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.util.FileManager;

public class ConstructTest {

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
        
        ExecContext context=new ExecContext(CQELSHOME, create);
        if(create) {
        	context.loadDefaultDataset(CQELSDATA+"/"+DATASIZE+".rdf");
            context.loadDataset("http://deri.org/floorplan/", CQELSDATA+"/floorplan.rdf");
            //context.loadDataset("http://deri.org/dblp/",path+"/10k.rdf");
        }
        
        
        int j=0;
        Pair<Long, Long> pair=new Pair<Long, Long>((long)0, (long)0);
		BufferedReader reader;
		ConstructListener cl = new ConstructListener(context, "http://deri.org/floorplan/") {
			
			@Override
			public void update(List<Triple> graph) {
				for(Triple t : graph) {
					System.out.println(t.getSubject() + " " + t.getPredicate() + " " + t.getObject());
				}
			}
		};
		
		try {
			reader = new BufferedReader(new FileReader(CQELSDATA+"/authors.text"));
			String name;
			while ((name = reader.readLine()) != null &&j++<noQ)   {
				if( RUNMODE.equalsIgnoreCase("single")&&(j<noQ)) continue;
//				ContinuousSelect cq=context.registerSelect(query(queryString,name));
//		        cq.register(new ResultLog());
			      
				ContinuousConstruct cq = context.registerConstruct(query(queryString, name));
				cq.register(cl);
				
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
        System.out.printf("matched value: " + cl.count);
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
			while ((strLine = reader.readLine()) != null &&i<STREAMSIZE)   {
				if(i%step==0) System.out.println("line "+i+" "+strLine +" "+System.currentTimeMillis());
			    String[] data=strLine.split(" ");
				context.engine().send(rfid,n(data[0]),n(data[1]),n(data[2]));	
				
				i++;
				
			 }
			
			
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
