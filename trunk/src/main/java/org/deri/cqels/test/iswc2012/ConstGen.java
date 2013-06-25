package org.deri.cqels.test.iswc2012;


import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;

public class ConstGen {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Model model=ModelFactory.createDefaultModel();
		long count=0;
		model.read("file:///test/iswc2012/data/sib1000.rdf");
		String queryString = "select ?uri where { ?uri a <http://www.ins.cwi.nl/sib/vocabulary/User>}" ;
		//String queryString = "select ?uri where { ?uri a <http://xmlns.com/foaf/0.1/Person>}" ;
		  Query query = QueryFactory.create(queryString) ;
		  QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
		  try {
		    ResultSet results = qexec.execSelect() ;
		    for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode x = soln.get("uri") ;       // Get a result variable by name.
		      count++;
		      System.out.println(count + " "+x);
		    }
		  } finally { qexec.close() ; }
	}

}
