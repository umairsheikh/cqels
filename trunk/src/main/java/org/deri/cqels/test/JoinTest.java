package org.deri.cqels.test;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.deri.cqels.data.HashMapping;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.BDBGraphPatternRouter;
import org.deri.cqels.engine.CQELSEngine;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.FilterExprRouter;
//import org.deri.cqels.engine.GraphPatternRouter;
import org.deri.cqels.engine.GroupRouter;
import org.deri.cqels.engine.IndexedTripleRouter;
import org.deri.cqels.engine.JoinRouter;
import org.deri.cqels.engine.RangeWindow;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.openjena.atlas.lib.Pair;

import com.espertech.esper.client.EPStatement;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
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
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVar;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.vocabulary.DC_11;

public class JoinTest {

	private static final String LOG_TXT = "/Users/danh/log.txt";
	/**
	 * @param args
	 */
	static String baseURI="http://deri.org/dblp/";
	static String floor="http://deri.org/floorplan/";
	
	static Node detected=Node.createURI("http://deri.org/floorplan/dectectedAt");
	static Node stream=Node.createURI("http://deri.org/locstreams");
	static StreamListener lit=new StreamListener();
	
	public static void main(String[] args) {
		//joinPattern("/test/50K");
		//loadGraph("/data/cqels/10M");
		//buildingCacheTest();
		//simpleJoinTest();
		//nestedJoinTest();
		indexedJoinTest();
		//groupByTest();
	}
	
	public static ExecContext initContext(){
		
		ExecContext context= new ExecContext("",false);
		context.setEngine(new CQELSEngine(context));
		context.createCache("/test/cache");
		context.createDataSet("/data/cqels/10k");
		
		return context;
	}
	public static void simpleJoinTest(){
		ExecContext context= initContext();
		
		Triple locTriple=new Triple(Var.alloc("person"), detected, Var.alloc("loc"));
		Quad quad=new Quad(stream,locTriple);
		
		OpTriple author= new OpTriple(new Triple(Var.alloc("person"), FOAF.name.asNode(), Var.alloc("name")));
		
		OpTriple loc= new OpTriple(locTriple);
		
		OpJoin join=(OpJoin)OpJoin.create(loc,author);
		
		//context.setPolicy(new FixedPlanner(context, join));
		
		BDBGraphPatternRouter authorRouter=new BDBGraphPatternRouter(context, author);
		IndexedTripleRouter locRouter= new IndexedTripleRouter(context, loc,  stream,new RangeWindow((long)1E9));
		EPStatement stmt=context.engine().addWindow(quad, ".win:length(1)");
		//JoinRouter joinRouter = new JoinRouter(context, join);
		stmt.setSubscriber(locRouter);
		
		//context.engine().setSubcriber(new StreamListener(), join);
		//context.engine().register(join, lit);
		stream(context,LOG_TXT);
	}
	
	public static void groupByTest(){
		ExecContext context= initContext();
		
		Triple locTriple=new Triple(Var.alloc("editor"), detected, Var.alloc("loc"));
		Quad quad=new Quad(stream,locTriple);
		
		OpTriple editor= new OpTriple(new Triple(Var.alloc("proc"),
								Node.createURI("http://swrc.ontoware.org/ontology#editor"), Var.alloc("editor")));
		
		OpTriple loc= new OpTriple(locTriple);
		

		ArrayList<Var> vars= new ArrayList<Var>();
		vars.add(Var.alloc("editor"));
		OpDistinct editorDistinct= (OpDistinct)OpDistinct.create(new OpProject(editor,vars));
		OpJoin join=(OpJoin)OpJoin.create(loc,editorDistinct);
		
		BDBGraphPatternRouter editorRouter=new BDBGraphPatternRouter(context, editorDistinct);
	
		EPStatement stmt1=context.engine().addWindow(quad, ".win:length(1)");
		IndexedTripleRouter locRouter2= new IndexedTripleRouter(context, loc,  stream,new RangeWindow((long)30E9));
		stmt1.setSubscriber(locRouter2);
		
		VarExprList groupV=new VarExprList();
		groupV.add(Var.alloc("loc"));
		List<ExprAggregator> aggs= new ArrayList<ExprAggregator>();
		
		aggs.add(new ExprAggregator(Var.alloc("noEditors"),new AggCountVar(new ExprVar("editor"))));
		OpGroup sum=new OpGroup(join, groupV, aggs);
		
		//JoinRouter joinRouter = new JoinRouter(context, join);

		//GroupRouter gRouter=new GroupRouter(context, sum);
		
		//context.setPolicy(new FixedPlanner(context, sum));

		//context.engine().register(sum, lit);
		stream(context,LOG_TXT);
	}
	
	public static void nestedJoinTest(){
		ExecContext context= initContext();
		
		Triple locTriple1=new Triple(Var.alloc("person1"), detected, Var.alloc("loc"));
		Triple locTriple2=new Triple(Var.alloc("person2"), detected, Var.alloc("loc"));
		Quad quad1=new Quad(stream,locTriple1);
		Quad quad2=new Quad(stream,locTriple2);
		
		OpTriple author1= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("person1")));
		OpTriple author2= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("person2")));
		
		OpTriple loc1= new OpTriple(locTriple1);
		OpTriple loc2= new OpTriple(locTriple2);
		
		OpJoin authorJoin=(OpJoin)OpJoin.create(author1,author2);
		Expr notEqual=new E_NotEquals(new ExprVar("person1"),new ExprVar("person2"));
		Op filter1=OpFilter.filter(notEqual, authorJoin);

		ArrayList<Var> vars= new ArrayList<Var>();
		vars.add(Var.alloc("person1"));
		vars.add(Var.alloc("person2"));
		OpProject authorProject = new OpProject(filter1, vars);
		OpDistinct authorDistinct= (OpDistinct)OpDistinct.create(authorProject);
		
		
		OpJoin join2=(OpJoin)OpJoin.create(loc1,loc2);
		Op filter2=OpFilter.filter(notEqual, join2);
		OpJoin join=(OpJoin)OpJoin.create(filter2,authorDistinct);
		//context.setPolicy(new FixedPlanner(context, join));
		
		BDBGraphPatternRouter coauthorRouter=new BDBGraphPatternRouter(context, authorDistinct);
	
		
		EPStatement stmt1=context.engine().addWindow(quad1, ".win:length(1)");
		EPStatement stmt2=context.engine().addWindow(quad2, ".win:length(1)");
		
		IndexedTripleRouter locRouter1= new IndexedTripleRouter(context, loc1,  stream,new RangeWindow((long)1E7));
		stmt1.setSubscriber(locRouter1);
		IndexedTripleRouter locRouter2= new IndexedTripleRouter(context, loc2,  stream,new RangeWindow((long)2E9));
		stmt2.setSubscriber(locRouter2);
		
		//FilterExprRouter filterRouter= new FilterExprRouter(context, (OpFilter)filter2); 		
		//JoinRouter joinRouter2 = new JoinRouter(context, join2);		
		//JoinRouter joinRouter = new JoinRouter(context, join);
		
		//context.engine().setSubcriber(new StreamListener(), join);
		
		//context.engine().register(join, lit);
		stream(context,LOG_TXT);
	}
	
	
	
	public static void indexedJoinTest(){
		ExecContext context= initContext();
		
		Triple locTriple1=new Triple(Var.alloc("person1"), detected, Var.alloc("loc"));
		Triple locTriple2=new Triple(Var.alloc("person2"), detected, Var.alloc("loc"));
		Quad quad1=new Quad(stream,locTriple1);
		Quad quad2=new Quad(stream,locTriple2);
		
		OpTriple author1= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("person1")));
		OpTriple author2= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("person2")));
		
		OpTriple loc1= new OpTriple(locTriple1);
		OpTriple loc2= new OpTriple(locTriple2);
		
		OpJoin authorJoin=(OpJoin)OpJoin.create(author1,author2);
		Expr notEqual=new E_NotEquals(new ExprVar("person1"),new ExprVar("person2"));
		Op filter1=OpFilter.filter(notEqual, authorJoin);

		ArrayList<Var> vars= new ArrayList<Var>();
		vars.add(Var.alloc("person1"));
		vars.add(Var.alloc("person2"));
		OpProject authorProject = new OpProject(filter1, vars);
		OpDistinct authorDistinct= (OpDistinct)OpDistinct.create(authorProject);
		
		
		OpJoin join2=(OpJoin)OpJoin.create(loc1,authorDistinct);
		
		OpJoin join=(OpJoin)OpJoin.create(join2,loc2);
		//context.setPolicy(new FixedPlanner(context, join));
		
		BDBGraphPatternRouter coauthorRouter=new BDBGraphPatternRouter(context, authorDistinct);
	
		
		EPStatement stmt1=context.engine().addWindow(quad1, ".win:length(1)");
		EPStatement stmt2=context.engine().addWindow(quad2, ".win:length(1)");
		
		IndexedTripleRouter locRouter1= new IndexedTripleRouter(context, loc1,  stream,new RangeWindow((long)1E7));
		stmt1.setSubscriber(locRouter1);
		IndexedTripleRouter locRouter2= new IndexedTripleRouter(context, loc2,  stream,new RangeWindow((long)2E9));
		stmt2.setSubscriber(locRouter2);
		
		//FilterExprRouter filterRouter= new FilterExprRouter(context, (OpFilter)filter2); 		
		//JoinRouter joinRouter2 = new JoinRouter(context, join2);		
		//JoinRouter joinRouter = new JoinRouter(context, join);
		
		//context.engine().setSubcriber(new StreamListener(), join);
		
		//context.engine().register(join, lit);
		stream(context,LOG_TXT);
	}
	

	
	public static void stream(ExecContext context, String file){
		OpTriple author= new OpTriple(new Triple(Var.alloc("person"), FOAF.name.asNode(), Var.alloc("name")));
		ArrayList<Node> list= new ArrayList<Node>();
		for(QueryIterator itr=Algebra.exec(author, context.getDataset());itr!=null&&itr.hasNext();)
			list.add(itr.next().get(Var.alloc("person")));
		long start=System.currentTimeMillis();
		BufferedReader reader;
		System.out.println("start sending");
		
		try {
			reader = new BufferedReader(new FileReader(file));
			String strLine;
			HashMap<String, Node> authors=new HashMap<String, Node>();
			int i=0, j=0,noT=(int)1E3;
			ArrayList<Pair<String, String>> streams= new ArrayList<Pair<String,String>>();
			 while ((strLine = reader.readLine()) != null &&i<noT)   {
				  String[] data=strLine.split(" ");
				if(!authors.containsKey(data[0]))  authors.put(data[0], list.get(j++)); 
				streams.add(new Pair<String,String>(data[0],data[2]));
				i++;
			 }
			 start=System.currentTimeMillis();
			 for(Pair<String,String> pair:streams){
				 //System.out.println("sending "+authors.get(pair.getLeft())+" "+Node.createURI(floor+pair.getRight()));
				 context.engine().send(stream, authors.get(pair.getLeft()),detected,Node.createURI(floor+pair.getRight()));
			 }
			 long el=(System.currentTimeMillis()-start);
			 System.out.println(" elapsed time " +el+" throughput " +noT*1000/el +" no results "+ lit.count + " delete "+IndexedTripleRouter.accT);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static void stream(ExecContext context){
		OpTriple author= new OpTriple(new Triple(Var.alloc("person"), FOAF.name.asNode(), Var.alloc("name")));
		ArrayList<Node> list= new ArrayList<Node>();
		for(QueryIterator itr=Algebra.exec(author, context.getDataset());itr!=null&&itr.hasNext();)
			list.add(itr.next().get(Var.alloc("person")));
		long start=System.currentTimeMillis();
		//System.out.println(list.size());

		for(int i=0;i<1E4;i++){
			//System.out.println(list.get(i%list.size())+" "+Node.createURI(baseURI+"location/"+i%2));
			context.engine().send(stream, list.get(i%100),detected,Node.createURI(baseURI+"location/"+i%100));
		}
		System.out.println(" elapsed time " +(System.currentTimeMillis()-start));
	}
	
	public static void buildingCacheTest(){
		ExecContext context= initContext();

		OpTriple author1= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("author1")));
		OpTriple author2= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("author2")));
		Op  join= OpJoin.create(author1, author2);
		//GraphPatternRouter router=new GraphPatternRouter(context, join);
		HashMap<Var, Long> hMap= new HashMap<Var, Long>();
		hMap.put(Var.alloc("author1"), context.engine().encode(Node.createURI("http://localhost/persons/Binita_Araque")));
		/*for(MappingIterator itr=router.searchBuff4Match(new HashMapping(context,hMap));itr.hasNext();){
			Mapping map=itr.next();
			System.out.println(context.engine().decode(map.get(Var.alloc("author2"))));
			
		}*/
	}
	public  static void joinPattern(String path){
		DatasetGraph dataset=TDBFactory.createDatasetGraph(path);
	
		OpTriple author1= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("author1")));
		OpTriple author2= new OpTriple(new Triple(Var.alloc("paper"), DC_11.creator.asNode(), Var.alloc("author2")));
		Op  join= OpJoin.create(author1, author2);
		
	
	
		for(QueryIterator itr=Algebra.exec(join, dataset);itr!=null&&itr.hasNext();){
			Binding binding=itr.next();
			try{
				System.out.println(binding.get(Var.alloc("author1"))+"-->"+binding.get(Var.alloc("author2")));
			}catch(Exception e){
					//e.printStackTrace();
					
				}
		}
		
	}
	
	

}
