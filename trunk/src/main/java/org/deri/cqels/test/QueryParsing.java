package org.deri.cqels.test;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.HeuristicRoutingPolicy;
import org.deri.cqels.lang.cqels.ParserCQELS;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.AlgebraGenerator;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterConjunction;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterDisjunction;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformFilterPlacement;
import com.hp.hpl.jena.sparql.algebra.optimize.TransformJoinStrategy;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.util.FileManager;



public class QueryParsing {
	
	public static void main( String[] args )
    {
    	
		FileManager filemanager = FileManager.get() ;
        String queryString = filemanager.readWholeFileAsUTF8("/data/cqels/q5.cqels") ;
       
        ExecContext context=new ExecContext("/test/cqels", false);
        //context.loadDefaultDataset("/data/cqels/10k.rdf");
        //context.loadDataset("http://deri.org/floorplan/", "/data/cqels/floorplan.rdf");
        Op op=context.registerSelect(queryString).getOp();
      
        System.out.println(op+"");
    }
}
