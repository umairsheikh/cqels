package org.deri.cqels.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.deri.cqels.engine.IndexedOnWindowBuff;
import org.openjena.atlas.iterator.IteratorConcat;
import org.openjena.atlas.lib.Pair;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueString;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.DC_11;

public class DataGenerator {
	private static final String DATASET = "/home/chanlevan/Internship/Data/Test/";
	private static final String OUT = "/home/chanlevan/Internship/Data/Backup/";
	static String floor="http://deri.org/floorplan/";
	static String detected=floor+"detectedAt";
	static String poke="http://facebook.com/voc/poke";
	public static void main( String[] args )
    {
		authorList();
		//convert();
		//convert2Facts();
		//floorPlan();
		//floorPlanDesc();
		//loadGraph(DATASET);
		//dblpEditor();
		//dblpOnlyEditor();
		//floorPlanNCoAuthor();
		//stream();
		//countEditor();
		
		//authorList();
		//loadFloor("/data/cqels/singlequery/100k/floorplan");
    }
	
	public static void authorList() {
		OpTriple author = new OpTriple(new Triple(Var.alloc("person"), FOAF.name.asNode(), Var.alloc("name")));
		ArrayList<Node> list= new ArrayList<Node>();
		DatasetGraph dataset=TDBFactory.createDatasetGraph(DATASET);
		for(QueryIterator itr = Algebra.exec(author, dataset);itr != null&&itr.hasNext();)
			list.add(itr.next().get(Var.alloc("name")));
		try {
			FileWriter writer=new FileWriter(OUT+"authors.text");
			int k=123;
			for(int i=0;i<123;i++){
				writer.write(list.get(i).getLiteral().getValue()+"\n");
				for(int j=0;j<3&&k<1000;j++){
					writer.write(list.get(k++).getLiteral().getValue()+"\n");
				}
			}
			for(;k<1000;){
				writer.write(list.get(k++).getLiteral().getValue()+"\n");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
	}
	
	public static void loadFloor(String path){
		DatasetGraph dataset=TDBFactory.createDatasetGraph(path);
		try {
			
			Model model=ModelFactory.createModelForGraph(dataset.getDefaultGraph());
			//model.write(System.out);
			BufferedReader reader=new BufferedReader(new FileReader("/Users/danh/floorplan.txt"));
			String strLine;
			HashMap<String, String> locs= new HashMap<String, String>();
			String base="http://deri.org/floorplan/";
			
			while ((strLine = reader.readLine()) != null)   {
				  String[] data=strLine.split(" ");
				  if(!locs.containsKey(data[0])) locs.put(data[0], data[0]);
				  if(!locs.containsKey(data[1])) locs.put(data[1], data[1]);
				  model.getGraph().add(new Triple(Node.createURI(base+data[0]), Node.createURI(base+"connected"),Node.createURI(base+data[1])));
				  model.getGraph().add(new Triple(Node.createURI(base+data[1]), Node.createURI(base+"connected"),Node.createURI(base+data[0])));
		     }			
			 			
			 for(Iterator<String> itr=locs.keySet().iterator();itr.hasNext();){
				 String loc=itr.next();	 
				 model.getGraph().add( new Triple(Node.createURI(base+loc),Node.createURI(base+"name"), Node.createLiteral("Location "+loc)));
				 model.getGraph().add( new Triple(Node.createURI(base+loc),Node.createURI(base+"desc"), Node.createLiteral("Description of location "+loc)));
			 }
			 model.write(new FileOutputStream("/data/cqels/floorplan.n3"),"N3");
			dataset.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	public  static void convert(){
		FileOutputStream fo;
		try {
			fo = new FileOutputStream("/data/cqels/50k.rdf");
			ModelFactory.createDefaultModel().read("file:///data/cqels/50k.n3","N3").write(fo,"RDF/XML");
			fo.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public  static void convert2Facts(){
		
		try {
			FileWriter fw = new FileWriter("/data/cqels/1M.p");
			Model model=ModelFactory.createDefaultModel().read("file:///data/cqels/1M.n3","N3");
			for(StmtIterator itr= model.listStatements();itr.hasNext();){
				Statement stm=itr.next();
				fw.write("rdf('"+stm.getSubject()+"','"+stm.getPredicate()+"','"+stm.getObject()+"').\n");
			}
			fw.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void loadGraph(String path){
		/*String q="select ?person ?name where {?person <"+FOAF.name.getURI()+"> ?name}";
		 Query query = QueryFactory.create(q) ;
	        System.out.println(query) ;
	        
	        // Generate algebra
	        Op op = Algebra.compile(query) ;
	        op = Algebra.optimize(op) ;
	        System.out.println(op) ;*/
		DatasetGraph dataset=TDBFactory.createDatasetGraph(path);
		//dataset=TDBFactory.createDatasetGraph(path);
		/*ArrayList<Node> list= new ArrayList<Node>();
		OpTriple op= new OpTriple(new Triple(Var.alloc("person"), FOAF.name.asNode(),Node.createLiteral("Paul Erdoes","",XSDDatatype.XSDstring)));
		for(QueryIterator itr=Algebra.exec(op, dataset);itr!=null&&itr.hasNext();)
			list.add(itr.next().get(Var.alloc("person")));
		System.out.println(list.size()+" "+list.get(0)+" ");*/
		
		
		/*try {
			ModelFactory.createModelForGraph(dataset.getDefaultGraph()).read(new FileReader("/data/cqels/100k.N3"),"http://deri.org/dblp/","N3");
			
			dataset.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
			
	}
	
	public static void floorPlan(){
		try{
			Model model=ModelFactory.createDefaultModel();
			BufferedReader reader=new BufferedReader(new FileReader("/Users/danh/floorplan.txt"));
			String strLine;
			 String base="http://deri.org/floorplan/";
			HashMap<String, String> locs= new HashMap<String, String>();
			 while ((strLine = reader.readLine()) != null)   {
				  String[] data=strLine.split(" ");
				  if(!locs.containsKey(data[0])) locs.put(data[0], data[0]);
				  if(!locs.containsKey(data[1])) locs.put(data[1], data[1]);
				  model.getGraph().add(new Triple(Node.createURI(base+data[0]), Node.createURI(base+"connected"),Node.createURI(base+data[1])));
				  model.getGraph().add(new Triple(Node.createURI(base+data[1]), Node.createURI(base+"connected"),Node.createURI(base+data[0])));
		     }			
			 			
			 for(Iterator<String> itr=locs.keySet().iterator();itr.hasNext();){
				 String loc=itr.next();	 
				 model.getGraph().add( new Triple(Node.createURI(base+loc),Node.createURI(base+"name"), Node.createLiteral("Location "+loc)));
			 }
			 model.setNsPrefix("fp", base);
			 model.write(new FileOutputStream("/Users/danh/floorplan.rdf"),"RDF/XML");
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void floorPlans(){
		try{
			FileWriter writer= new FileWriter(OUT+"floorplan.txt");
			BufferedReader reader=new BufferedReader(new FileReader("/Users/danh/floorplan.txt"));
			String strLine;
			 String base="http://deri.org/floorplan/";
			HashMap<String, String> locs= new HashMap<String, String>();
			 while ((strLine = reader.readLine()) != null)   {
				  String[] data=strLine.split(" ");
				  if(!locs.containsKey(data[0])) locs.put(data[0], data[0]);
				  if(!locs.containsKey(data[1])) locs.put(data[1], data[1]);
				  writer.write("floorplan('"+base+data[0]+"','"+base+"connected','"+base+data[1]+"').\n");
				  writer.write("floorplan('"+base+data[1]+"','"+base+"connected','"+base+data[0]+"').\n");
		     }			
			 writer.close();			
			 
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void stream(){
		OpTriple author= new OpTriple(new Triple(Var.alloc("person"), FOAF.name.asNode(), Var.alloc("name")));
		ArrayList<Node> list= new ArrayList<Node>();
		DatasetGraph dataset=TDBFactory.createDatasetGraph(DATASET);
		for(QueryIterator itr=Algebra.exec(author, dataset);itr!=null&&itr.hasNext();)
			list.add(itr.next().get(Var.alloc("person")));
		//int [] st= {100,500,1000,2000,5000,10000,50000};
		int [] st= {20000,100000};
		for(int noT:st){
			try {
				BufferedReader  reader = new BufferedReader(new FileReader("/Users/danh/log.txt"));
				FileWriter w= new FileWriter(OUT+"stream_"+noT+".P");
				FileWriter w1= new FileWriter(OUT+"rfid_"+noT+".stream");
				String strLine;
				HashMap<String, Node> authors=new HashMap<String, Node>();
				HashMap<String, String> lastLoc=new HashMap<String, String>();
				int i=0, j=0;
				ArrayList<Pair<String,String>> pokes=new ArrayList<Pair<String,String>>();
				 while ((strLine = reader.readLine()) != null &&i<noT)   {
					  String[] data=strLine.split(" ");
					if(!authors.containsKey(data[0]))  authors.put(data[0], list.get(j++)); 
					w.write("rdf('"+authors.get(data[0])+"','"+detected+"','"+floor+data[2]+"').\n");
					w1.write(authors.get(data[0])+" "+detected+" "+floor+data[2]+"\n");
					i++;
					if(i%3==0){
						if(Math.random()>7&&lastLoc.containsKey(data[2])){
							pokes.add(new Pair<String,String>(authors.get(lastLoc.get(data[2]))+"",authors.get(data[0])+""));
							lastLoc.clear();
						}
						else
							pokes.add(new Pair<String,String>(list.get((int)Math.round(j*Math.random()))+"", authors.get(data[0])+""));
					}
					lastLoc.put(data[2], data[0]);
					
				 }
				reader.close(); 
				w.close();
				w1.close();
				
				System.out.println("no "+j);
				
				reader = new BufferedReader(new FileReader(OUT+"rfid_"+noT+".stream"));
				 w= new FileWriter(OUT+"streamwithpoke_"+noT+".P");
				w1= new FileWriter(OUT+"rfidwithpoke_"+noT+".stream");
				i=0;j=0;
				while ((strLine = reader.readLine())!=null){
					String[] data=strLine.split(" ");
					if(i%3==0&&j<pokes.size()){
						Pair<String,String> p=pokes.get(j);
						w.write("rdf('"+p.getLeft()+"','"+poke+"','"+p.getRight()+"').\n");
						w1.write(p.getLeft()+" "+poke+" "+p.getRight()+"\n");
						j++;
					}
					w.write("rdf('"+data[0]+"','"+detected+"','"+data[2]+"').\n");
					w1.write(strLine+"\n");
					
					i++;
				}
				reader.close(); 
				w.close();
				w1.close();
			
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public static void floorPlanDesc(){
		try{
			FileWriter writer= new FileWriter(OUT+"floorplandesc.txt");
			BufferedReader reader=new BufferedReader(new FileReader("/Users/danh/floorplan.txt"));
			String strLine;
			 String base="http://deri.org/floorplan/";
			HashMap<String, String> locs= new HashMap<String, String>();
			 while ((strLine = reader.readLine()) != null)   {
				  String[] data=strLine.split(" ");
				  for(int i=0;i<2;i++){
					  if(!locs.containsKey(data[i])){
						  locs.put(data[i], data[i]);
						  writer.write("floorplan('"+base+data[i]+"','"+base+"name','Location "+data[i]+"').\n");
						  writer.write("floorplan('"+base+data[i]+"','"+base+"desc','Location description of "+data[i]+"').\n");
					  }
				  }
				  
		     }			
			 writer.close();			
			 
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void countEditor(){
		try{
			FileWriter writer= new FileWriter(OUT+"counteditor.txt");
			BufferedReader reader=new BufferedReader(new FileReader("/Users/danh/floorplan.txt"));
			String strLine;
			 String base="http://deri.org/floorplan/";
			HashMap<String, String> locs= new HashMap<String, String>();
			 while ((strLine = reader.readLine()) != null)   {
				  String[] data=strLine.split(" ");
				  for(int i=0;i<2;i++){
					  if(!locs.containsKey(data[i])){
						  locs.put(data[i], data[i]);
						  writer.write("count_editor('"+base+data[i]+"',0).\n");
					  }
				  }
				  
		     }			
			 writer.close();			
			 
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void output(DatasetGraph dataset, Writer w, String var1, String var2, String pred) throws IOException{
		Var v1=Var.alloc(var1);
		Var v2=Var.alloc(var2);
		OpTriple p= new OpTriple(new Triple(v1, Node.createURI(pred), v2));
		
		for(QueryIterator itr=Algebra.exec(p, dataset);itr!=null&&itr.hasNext();){
			Binding b=itr.next();
			w.write("rdf('"+b.get(v1)+"','"+pred+"','"+b.get(v2)+"').\n");
		}
	}
	public static void dblpEditor(){
		try{
			FileWriter writer= new FileWriter(OUT+"query3.dat");
			DatasetGraph dataset=TDBFactory.createDatasetGraph(DATASET);
			output(dataset,writer,"paper","author",DC_11.creator.getURI());
			output(dataset,writer,"author","name",FOAF.name.getURI());
			output(dataset,writer,"proc","editor","http://swrc.ontoware.org/ontology#editor");
			output(dataset,writer,"paper","editor","http://purl.org/dc/terms/partOf");
			
			 writer.close();			
			 
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void dblpOnlyEditor(){
		try{
			FileWriter writer= new FileWriter(OUT+"dblponlyeditor.txt");
			DatasetGraph dataset=TDBFactory.createDatasetGraph(DATASET);
			output(dataset,writer,"editor","name",FOAF.name.getURI());
			output(dataset,writer,"proc","editor","http://swrc.ontoware.org/ontology#editor");
			writer.close();			
			 
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void floorPlanNCoAuthor(){
		try{
			FileWriter writer= new FileWriter(OUT+"query4.dat");
			BufferedReader reader=new BufferedReader(new FileReader("/Users/danh/floorplan.txt"));
			String strLine;
			 String base="http://deri.org/floorplan/";
			HashMap<String, String> locs= new HashMap<String, String>();
			 while ((strLine = reader.readLine()) != null)   {
				  String[] data=strLine.split(" ");
				  if(!locs.containsKey(data[0])) locs.put(data[0], data[0]);
				  if(!locs.containsKey(data[1])) locs.put(data[1], data[1]);
				  writer.write("floorplan('"+base+data[0]+"','"+base+"connected','"+base+data[1]+"').\n");
				  writer.write("floorplan('"+base+data[1]+"','"+base+"connected','"+base+data[0]+"').\n");
				  
		     }	
			 DatasetGraph dataset=TDBFactory.createDatasetGraph(DATASET);
			 output(dataset,writer,"paper","author",DC_11.creator.getURI());
			 output(dataset,writer,"author","name",FOAF.name.getURI());
			 writer.close();			
			 
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
}
