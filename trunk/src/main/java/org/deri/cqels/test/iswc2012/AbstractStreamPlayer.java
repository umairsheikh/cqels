package org.deri.cqels.test.iswc2012;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.Reader;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;

import com.hp.hpl.jena.n3.turtle.TurtleEventHandler;
import com.hp.hpl.jena.n3.turtle.TurtleParseException;
import com.hp.hpl.jena.n3.turtle.parser.ParseException;
import com.hp.hpl.jena.n3.turtle.parser.TokenMgrError;
import com.hp.hpl.jena.n3.turtle.parser.TurtleParser;
import com.hp.hpl.jena.shared.JenaException;
import com.hp.hpl.jena.util.FileUtils;

public abstract class AbstractStreamPlayer extends RDFStream implements Runnable {
	protected String stFile;
	TurtleEventHandler handler;
	public AbstractStreamPlayer( ExecContext context,String iri, String stFile){
		super(context,iri);
		this.stFile=stFile;
		setHandler();
	}
	
	public void run() {
		FileReader reader;
		try {
			reader = new FileReader(stFile);
			parse(getURI(), reader);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public abstract void setHandler();
	public abstract long getTriples();
	
	public void parse(String baseURI, InputStream in)
	 {
	        Reader reader = FileUtils.asUTF8(in) ;
	        parse( baseURI, reader) ;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}
	
	public  void parse(String baseURI, Reader reader)
	 {
	        // Nasty things happen if the reader is not UTF-8.
	      try {
	            TurtleParser parser = new TurtleParser(reader) ;
	            parser.setEventHandler(handler) ;
	            parser.setBaseURI(baseURI) ;
	            parser.parse() ;
	        }

	        catch (ParseException ex)
	        { throw new TurtleParseException(ex.getMessage()) ; }

	        catch (TokenMgrError tErr)
	        { throw new TurtleParseException(tErr.getMessage()) ; }

	        catch (TurtleParseException ex) { throw ex ; }
	        
	        catch (JenaException ex)  { throw new TurtleParseException(ex.getMessage(), ex) ; }
	        catch (Error err)
	        {
	            //System.out.println("error"+ err.getMessage());
	        	throw new TurtleParseException(err.getMessage() , err) ;
	        }
	        catch (Throwable th)
	        {
	            throw new TurtleParseException(th.getMessage(), th) ;
	        }
	    }


}
