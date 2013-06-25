package org.deri.cqels.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


import org.deri.cqels.engine.BDBGraphPatternRouter;
import org.deri.cqels.engine.CQELSEngine;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.GroupRouter;
import org.deri.cqels.engine.IndexedTripleRouter;
import org.deri.cqels.engine.JoinRouter;
import org.deri.cqels.engine.RangeWindow;
import org.deri.cqels.engine.TripleWindow;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprVar;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountDistinct;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVar;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.DC_11;

public class ISWCEvaluation {

	/**
	 * @param args
	 */
	static final int Q1=1,Q2=2,Q3=3,Q4=4,Q5=5;
	
	static String baseURI="http://deri.org/dblp/";
	static String lv="http://deri.org/floorplan/";
	static Node detected=Node.createURI(lv+"detectedAt");
	static Node connected=Node.createURI(lv+"connected");
	static Node lvName=Node.createURI(lv+"name");
	static Node lvDesc=Node.createURI(lv+"desc");
	static Node rfid=Node.createURI("http://deri.org/streams/rfid");
	static Node partOf=Node.createURI(DCTerms.NS+"partOf");
	static Node editorPred=Node.createURI("http://swrc.ontoware.org/ontology#editor");
	static StreamListener lit=new StreamListener();
	static String path;
	static Var person=Var.alloc("person"),person1=Var.alloc("person1"),person2=Var.alloc("person2"),
		loc=Var.alloc("loc"),loc1=Var.alloc("loc1"),loc2=Var.alloc("loc2"),
		locName=Var.alloc("locName"),locDesc=Var.alloc("locDesc"),paper=Var.alloc("paper"),proc=Var.alloc("proc"),
		name=Var.alloc("name"),	authorName=Var.alloc("authorName"),coAuthorName=Var.alloc("coAuthorName"),
		editor=Var.alloc("editor"),editorName=Var.alloc("editorName"),author=Var.alloc("author"),coauthor=Var.alloc("coAuthor");
	static DatasetGraph floorDS;
	
	static  int TRIPLES=(int)5E4;
	public static void main(String[] args) {
		 path=args[0]+"/";
		 floorDS=TDBFactory.createDatasetGraph(path+"floorplan");  
		//singleQuery(1, "Paul Erdoes");
		 multiQueries(1, 5);
	}
	
	
	public static ExecContext initContext(String query){
		cleanNCreate(path+query);
		ExecContext context= new ExecContext("",false);
		context.setEngine(new CQELSEngine(context));
		context.createCache(path+query);
		context.createDataSet(path+"dblp");		
		return context;
	}
	
	static void cleanNCreate(String path){
		deleteDir(new File(path));
		if(!(new File(path)).mkdir()){
			System.out.println("can not create working directory"+path);
		}
	}
	
	public static boolean deleteDir(File dir) {
	        if (dir.isDirectory()) {
	            String[] children = dir.list();
	            for (int i=0; i<children.length; i++) {
	                boolean success = deleteDir(new File(dir, children[i]));
	                if (!success) {
	                	System.out.println("can not delete" +dir);
	                    return false;
	                }
	            }
	        }
	        return dir.delete();
	 }
	public static void record(ExecContext context,String query, Op root){
		//context.setPlaner(new FixedPlanner(context, root));
		SelectResultLog log=new SelectResultLog(context);
		//context.engine().register(root, log);
	}
	
	public static void switchQ(ExecContext context,int i,String name){
		switch (i){
			case Q1: record(context,"query"+i,query1(context,name));
				break;
			case Q2: record(context,"query"+i,query2(context));
			break;
			case Q3: record(context,"query"+i,query3(context,name));
			break;
			case Q4: record(context,"query"+i,query4(context,name));
			break;
			case Q5: record(context,"query"+i,query5(context,name));
			break;
		}
	}
	
	public static void singleQuery(int i,String name){
		ExecContext context= initContext("query"+i);
		switchQ(context, i, name);
		stream(context,"query"+i);
		
	}
	
	public static void multiQueries(int i,int k){
		
		ExecContext context= initContext("query"+i);
		int j=0;
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(path+"authors.text"));
			String name;
			while ((name = reader.readLine()) != null &&j++<k)   {
				switchQ(context, i, name);
			 }
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		stream(context,"query"+i);
	}
	
	public static Node lit(String st){
		return Node.createLiteral(st,"",XSDDatatype.XSDstring);
	}
	public static Op query1(ExecContext context,String name){
		
		OpTriple personOp= new OpTriple(new Triple(person, FOAF.name.asNode(), lit(name)));
		BDBGraphPatternRouter personRouter=new BDBGraphPatternRouter(context, personOp);
		
		OpTriple locNameOp=new OpTriple(new Triple(loc, lvName, locName));
		OpTriple locDescOp=new OpTriple(new Triple(loc, lvDesc, locDesc));
		OpJoin locJoin=(OpJoin)OpJoin.create(locNameOp,locDescOp);
		
		BDBGraphPatternRouter locRouterDesc=new BDBGraphPatternRouter(context, locJoin,floorDS);
		
		
		
		Triple locTriple=new Triple(person, detected, loc);
		OpTriple locOp= new OpTriple(locTriple);
		Quad quad=new Quad(rfid,locTriple);		
		IndexedTripleRouter locRouter= new IndexedTripleRouter(context, locOp,  rfid,new RangeWindow((long)1E3));
		EPStatement stmt=context.engine().addWindow(quad, ".win:length(1)");
		stmt.setSubscriber(locRouter);
		
		OpJoin join1=(OpJoin)OpJoin.create(locOp,personOp);
		//JoinRouter joinRouter1 = new JoinRouter(context, join1);
		
		OpJoin join2=(OpJoin)OpJoin.create(join1,locJoin);
		//JoinRouter joinRouter2 = new JoinRouter(context, join2);
		
		return join2;
	}
	
	public static Op query2(ExecContext context){
		
		OpTriple connectedOp=new OpTriple(new Triple(loc1, connected, loc2));
		
		BDBGraphPatternRouter locConnectedRouter=new BDBGraphPatternRouter(context, connectedOp,floorDS);
		
		Triple locTriple1=new Triple(person1, detected, loc1);
		OpTriple locOp1= new OpTriple(locTriple1);
		Quad quad1=new Quad(rfid,locTriple1);		
		IndexedTripleRouter locRouter1= new IndexedTripleRouter(context, locOp1,  rfid,new RangeWindow((long)1E3));
		EPStatement stmt1=context.engine().addWindow(quad1, ".win:length(1)");
		stmt1.setSubscriber(locRouter1);
		
		Triple locTriple2=new Triple(person2, detected, loc2);
		OpTriple locOp2= new OpTriple(locTriple2);
		Quad quad2=new Quad(rfid,locTriple2);		
		IndexedTripleRouter locRouter2= new IndexedTripleRouter(context, locOp2,  rfid,new RangeWindow((long)3E9));
		EPStatement stmt2=context.engine().addWindow(quad2, ".win:length(1)");
		stmt2.setSubscriber(locRouter2);
		
		OpJoin join1=(OpJoin)OpJoin.create(locOp1,connectedOp);
		//JoinRouter joinRouter1 = new JoinRouter(context, join1);
		
		OpJoin join2=(OpJoin)OpJoin.create(join1,locOp2);
		//JoinRouter joinRouter2 = new JoinRouter(context, join2);
		
		return join2;
	}
	
	
	public static Op query3(ExecContext context,String name){
		
		Triple locTriple1=new Triple(author, detected, loc);
		OpTriple locOp1= new OpTriple(locTriple1);
		Quad quad1=new Quad(rfid,locTriple1);		
		IndexedTripleRouter locRouter1= new IndexedTripleRouter(context, locOp1,  rfid,new TripleWindow(1));
		EPStatement stmt1=context.engine().addWindow(quad1, ".win:length(1)");
		stmt1.setSubscriber(locRouter1);
		
		Triple locTriple2=new Triple(coauthor, detected, loc);
		OpTriple locOp2= new OpTriple(locTriple2);
		Quad quad2=new Quad(rfid,locTriple2);		
		IndexedTripleRouter locRouter2= new IndexedTripleRouter(context, locOp2,  rfid,new RangeWindow((long)5E9));
		EPStatement stmt2=context.engine().addWindow(quad2, ".win:length(1)");
		stmt2.setSubscriber(locRouter2);
		
		BasicPattern bgp= new BasicPattern();
		
		bgp.add(new Triple(paper, DC_11.creator.asNode(), author));
		bgp.add(new Triple(author, FOAF.name.asNode(), lit(name)));
		bgp.add(new Triple(paper, DC_11.creator.asNode(), coauthor));	
		bgp.add(new Triple(coauthor, FOAF.name.asNode(), coAuthorName));
		
		OpBGP coauthorJoin=new OpBGP(bgp);
		
		Expr notEqual=new E_NotEquals(new ExprVar(author),new ExprVar(coauthor));
		Op filter1=OpFilter.filter(notEqual, coauthorJoin);
		//project author,coauthor and coauthorName
		OpProject authorProject = new OpProject(filter1, Arrays.asList(author,coauthor,coAuthorName));
		OpDistinct authorDistinct= (OpDistinct)OpDistinct.create(authorProject);
		BDBGraphPatternRouter coauthorRouter=new BDBGraphPatternRouter(context, authorDistinct);
		
		OpJoin join1=(OpJoin)OpJoin.create(locOp1,authorDistinct);
		//JoinRouter joinRouter1 = new JoinRouter(context, join1);
	
		
		OpJoin join2=(OpJoin)OpJoin.create(join1,locOp2);
		//JoinRouter joinRouter = new JoinRouter(context, join2);
	
		return join2;
	}
	
	public static Op query4(ExecContext context,String name){
		
		Triple locTriple1=new Triple(author, detected, loc1);
		OpTriple locOp1= new OpTriple(locTriple1);
		Quad quad1=new Quad(rfid,locTriple1);		
		IndexedTripleRouter locRouter1= new IndexedTripleRouter(context, locOp1,  rfid,new TripleWindow(1));
		EPStatement stmt1=context.engine().addWindow(quad1, ".win:length(1)");
		stmt1.setSubscriber(locRouter1);
		
		Triple locTriple2=new Triple(editor, detected, loc2);
		OpTriple locOp2= new OpTriple(locTriple2);
		Quad quad2=new Quad(rfid,locTriple2);		
		IndexedTripleRouter locRouter2= new IndexedTripleRouter(context, locOp2,  rfid,new RangeWindow((long)15E9));
		EPStatement stmt2=context.engine().addWindow(quad2, ".win:length(1)");
		stmt2.setSubscriber(locRouter2);
		
		BasicPattern bgp= new BasicPattern();
		bgp.add(new Triple(paper, DC_11.creator.asNode(), author));
		bgp.add(new Triple(author, FOAF.name.asNode(), lit(name)));
		bgp.add(new Triple( proc, editorPred,editor));	
		bgp.add(new Triple(editor, FOAF.name.asNode(), editorName));
		bgp.add(new Triple(paper,partOf,proc));
		OpBGP bgpOp= new OpBGP(bgp);
		Expr notEqual=new E_NotEquals(new ExprVar(author),new ExprVar(coauthor));
		Op filter=OpFilter.filter(notEqual, bgpOp);
		//project,author,editor,editorName
		OpProject authorEditorProject = new OpProject(filter,Arrays.asList(author,editor,editorName));
		OpDistinct authorEditorDistinct= (OpDistinct)OpDistinct.create(authorEditorProject);
		BDBGraphPatternRouter coauthorRouter=new BDBGraphPatternRouter(context, authorEditorDistinct);
		
		OpJoin join1=(OpJoin)OpJoin.create(locOp1,authorEditorDistinct);
		//JoinRouter joinRouter1 = new JoinRouter(context, join1);
		
		OpTriple connectedOp=new OpTriple(new Triple(loc1, connected, loc2));
		BDBGraphPatternRouter locConnectedRouter=new BDBGraphPatternRouter(context, connectedOp,floorDS);
		
		OpJoin join2=(OpJoin)OpJoin.create(join1, connectedOp);
		//JoinRouter joinRouter2 = new JoinRouter(context, join2);
		
		OpJoin join3=(OpJoin)OpJoin.create(join2,locOp2);
		//JoinRouter joinRouter3 = new JoinRouter(context, join3);
	
		return join3;
	}

	public static Op query5(ExecContext context,String name){
		
		Triple locTriple1=new Triple(author, detected, loc1);
		OpTriple locOp1= new OpTriple(locTriple1);
		Quad quad1=new Quad(rfid,locTriple1);		
		IndexedTripleRouter locRouter1= new IndexedTripleRouter(context, locOp1,  rfid,new TripleWindow(1));
		EPStatement stmt1=context.engine().addWindow(quad1, ".win:length(1)");
		stmt1.setSubscriber(locRouter1);
		
		Triple locTriple2=new Triple(coauthor, detected, loc2);
		OpTriple locOp2= new OpTriple(locTriple2);
		Quad quad2=new Quad(rfid,locTriple2);		
		IndexedTripleRouter locRouter2= new IndexedTripleRouter(context, locOp2,  rfid,new RangeWindow((long)5E9));
		EPStatement stmt2=context.engine().addWindow(quad2, ".win:length(1)");
		stmt2.setSubscriber(locRouter2);
		
		BasicPattern bgp= new BasicPattern();
		
		bgp.add(new Triple(paper, DC_11.creator.asNode(), author));
		bgp.add(new Triple(author, FOAF.name.asNode(), lit(name)));
		bgp.add(new Triple(paper, DC_11.creator.asNode(), coauthor));	
		
		OpBGP coauthorJoin=new OpBGP(bgp);
		
		Expr notEqual=new E_NotEquals(new ExprVar(author),new ExprVar(coauthor));
		Op filter1=OpFilter.filter(notEqual, coauthorJoin);
		//project author,coauthor and coauthorName
		OpProject authorProject = new OpProject(filter1, Arrays.asList(author,coauthor));
		OpDistinct authorDistinct= (OpDistinct)OpDistinct.create(authorProject);
		BDBGraphPatternRouter coauthorRouter=new BDBGraphPatternRouter(context, authorDistinct);
		
		OpJoin join1=(OpJoin)OpJoin.create(locOp1,authorDistinct);
		//JoinRouter joinRouter1 = new JoinRouter(context, join1);
	
		OpTriple connectedOp=new OpTriple(new Triple(loc1, connected, loc2));
		BDBGraphPatternRouter locConnectedRouter=new BDBGraphPatternRouter(context, connectedOp,floorDS);
		
		OpJoin join2=(OpJoin)OpJoin.create(join1,connectedOp);
		//JoinRouter joinRouter2 = new JoinRouter(context, join2);
		
		OpJoin join3=(OpJoin)OpJoin.create(join2,locOp2);
		//JoinRouter joinRouter3 = new JoinRouter(context, join3);
		
		VarExprList groupV=new VarExprList(Arrays.asList(loc2));
		List<ExprAggregator> aggs= new ArrayList<ExprAggregator>();
		
		aggs.add(new ExprAggregator(Var.alloc("nocoauthors"),new AggCountVarDistinct(new ExprVar(coauthor))));
		OpGroup count=new OpGroup(join3, groupV, aggs);
	
		//GroupRouter gRouter=new GroupRouter(context, count);
		
		return count;
				
	}
	
	public static  Node n(String st){
		return Node.createURI(st);
	}
	
	public static void stream(ExecContext context,String query) {
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(path+"stream/rfid_50000.stream"));
			FileWriter writer = new FileWriter(path+query+".log");
			String strLine;
			 long start=System.nanoTime();
			 int i=0,noT=(int)5E0;
			 System.out.println("start");
			while ((strLine = reader.readLine()) != null &&i<noT)   {
			    String[] data=strLine.split(" ");
				context.engine().send(rfid,n(data[0]),n(data[1]),n(data[2]));
				
				i++;
				if(i%1E3==0) System.out.println("line "+i+" "+strLine +" "+System.currentTimeMillis());
			 }
			
			
			 long end=System.nanoTime();
			 try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 
			 long el=Math.max(end, SelectResultLog.end)-start;
			 writer.write(el/TRIPLES+"");
			 writer.write("\n");
			 writer.write(""+SelectResultLog.count);
			 writer.close();
			 System.out.println("stop after " +el +" nano seconds and "+IndexedTripleRouter.accT +" nanoseconds for deleting");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
