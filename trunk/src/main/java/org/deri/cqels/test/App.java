package org.deri.cqels.test;


import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.objectfile.ObjectFile;
import com.hp.hpl.jena.tdb.index.Index;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;

import com.hp.hpl.jena.tdb.nodetable.NodeTableNative;
import com.hp.hpl.jena.tdb.store.NodeId;
import com.hp.hpl.jena.tdb.sys.SystemTDB;

/**
 * Hello world!
 *
 */
public class App 
{
    static final int NO = 1000000;
    
    public static void nodeTableTest(NodeTable table) {
    	NodeId[] ids= new NodeId[NO];
    	
    	 long start=System.currentTimeMillis();
         
    	 
    	for(int i = 0; i < NO; i++) {
    		ids[i]=table.getNodeIdForNode(Node.createURI("http://deri.org/page/"+i));
    		
    	}
    	
    	System.out.println(NO+ "  takes " +(System.currentTimeMillis()-start));
    	
    	start=System.currentTimeMillis();
    	
    	for(int i=0;i<NO;i++){
    		table.getNodeIdForNode(Node.createURI("http://deri.org/page/"+i));
    		
    	}
    	
    	System.out.println(NO+ "again  takes " +(System.currentTimeMillis()-start));
    	
    	start=System.currentTimeMillis();
    	
    	for(int i=0;i<NO;i++){
    		if(!(ids[i].equals(table.getNodeIdForNode(Node.createURI("http://deri.org/page/"+i)))))
    			System.out.println(Node.createURI("http://deri.org/page/"+i));
    	}
    	System.out.println(NO+ " search  takes " +(System.currentTimeMillis()-start));
    }
    
    public static void memNodeTable(){
    	Index nodeToId = IndexBuilder.mem().newIndex(FileSet.mem(), SystemTDB.nodeRecordFactory) ;
        ObjectFile objects = FileFactory.createObjectFileMem() ;
        nodeTableTest(new NodeTableNative(nodeToId, objects)) ;
    }
    
    public static void fileNodeTable(){
    	Index nodeToId = IndexBuilder.createIndex(new FileSet("/test","node"), SystemTDB.nodeRecordFactory) ;
        ObjectFile objects = FileFactory.createObjectFileDisk("/test/obj");
        nodeTableTest(new NodeTableNative(nodeToId, objects)) ;
    }
    
	public static void main( String[] args )
    {
    	
		memNodeTable();
        
    	
    }
}
