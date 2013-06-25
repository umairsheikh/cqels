package org.deri.cqels.test.iswc2012;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;


import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.FileManager;

public class CQELSMax {

	/**
	 * @param args
	 */
	
	static String DATA,HOME,OUTPUT,strLen,noU;
	static ExecContext context;
	static long count=0,triples=0;
	static ArrayList<AbstractStreamPlayer> streams=new ArrayList<AbstractStreamPlayer>();
	public static List<Thread> initStreams(String propFile){
		ArrayList<Thread> players= new ArrayList<Thread>();
		try {
			
			BufferedReader buff=new BufferedReader(new FileReader(propFile));
			String line=buff.readLine();
			boolean maxrate=true;
			while (line!=null){
				String[] props= line.split(" ");
				String strFile=props[1];
				strFile=strFile.replace("{STRLEN}", strLen);
				strFile=strFile.replace("{NOUSER}", noU);
				AbstractStreamPlayer stream;
				if(maxrate){ 
					stream=new StreamPlayerMax(context, props[0],DATA+"/"+strFile);
					maxrate=false;
				}
				else stream=new StreamPlayer(context, props[0],DATA+"/"+strFile,Integer.parseInt(props[2]));
				players.add(new Thread(stream));
				streams.add(stream);
				line=buff.readLine();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return players;
	}
	
	public static void main(String[] args) throws InterruptedException {
		
		if(args.length<7) System.exit(-1);
		
		String q=args[0];
		HOME=args[1];
		DATA=args[2];
		OUTPUT=args[3];
		noU=args[4];
		strLen=args[5];
		
		boolean loadStaticData=Boolean.parseBoolean(args[6]);
		context=new ExecContext(HOME, true);
		System.out.println("running query "+q);
		System.out.println("Working directory "+HOME);
		System.out.println("Input Data" +DATA);
		System.out.println("Output directory  "+OUTPUT);
		List<Thread> players=initStreams(HOME+"/"+q+".prop");
		String outFN=OUTPUT+"/"+q+"/"+q+"-"+noU+"-"+strLen+"-0.out";
		File file=new File(outFN);
		file.delete();
		
		try {
			final PrintStream out= new PrintStream(new File(outFN));
		if(loadStaticData)
			context.loadDefaultDataset(DATA+"/sib"+noU+".rdf");
		
		FileManager filemanager = FileManager.get() ;
        String query = filemanager.readWholeFileAsUTF8(HOME+"/"+q+".cqels") ;
        System.out.println(query);
        ContinuousSelect selQuery=context.registerSelect(query);
		selQuery.register(new ContinuousListener() {
			
			public void update(Mapping mapping) {
				String result="";
				for(Iterator<Var> vars=mapping.vars();vars.hasNext();)
					result+=" "+context.engine().decode(mapping.get(vars.next()));
				out.println(result);
				count++;
			}
			public void decode(){};
			public void printf(){};
		});
		
		
		long start=System.currentTimeMillis();
		for(Thread t:players) t.start();
		 while(isAlive(players)){
			 Thread.sleep(100);
		 }
		 long elapsed=(System.currentTimeMillis()-start);
		 out.println("Number of results "+count +" in "+elapsed +" milliseconds");
		 long triples=0;
		 for(AbstractStreamPlayer stream:streams)
			 triples+=stream.getTriples();
		 out.println("Number of triples streamed :" +triples+" with throughput "+((1000*triples)/elapsed));
		 out.flush();
		 
		 try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static boolean isAlive(List<Thread> players){
		for(Thread t:players) 
			if(t.isAlive()) return true;
		return false;
	}
}
