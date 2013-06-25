package org.deri.cqels.test;

import java.util.HashMap;
import java.util.Iterator;

import org.deri.cqels.data.HashMapping;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ExecContext;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;

import com.hp.hpl.jena.sparql.core.Var;

public class RoutingTest {

	/**
	 * @param args
	 */
	
	static EPServiceProvider provider= EPServiceProviderManager.getDefaultProvider();
	static ExecContext context= new ExecContext("",false);
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		EPStatement stm=provider.getEPAdministrator().createEPL("select * from org.deri.cqels.data.Mapping(opID=2).win:time(120 seconds)");
		EPStatement stm1=provider.getEPAdministrator().createEPL("select * from org.deri.cqels.data.Mapping(opID=1).win:time(60 seconds)");
		Subscriber sub=new RoutingTest.Subscriber(stm1);
		stm.setSubscriber(sub);
		long start=System.currentTimeMillis(), count=0;;
		for(int i=0; i<1E7;i++){
			//System.out.println( "i= " +i+ "opID="+i%30);
			send(hashMap(i%10+1,i%30));
			
			/*for(Iterator<EventBean> itr=stm.iterator();itr.hasNext();){
				Mapping mapping=(Mapping)itr.next().getUnderlying();
				//System.out.println(mapping.getClass()+" "+ mapping.get(Var.alloc("1")));
				count++;
			}*/
		}
		
		System.out.println(sub.count()+ " Elapsed time"+(System.currentTimeMillis()-start));
	}
	
	public static HashMapping hashMap(int no,int opID){
		HashMap<Var, Long> hMap= new HashMap<Var, Long>();
		for(int i=0;i<no;i++)
			hMap.put(Var.alloc(""+i),Math.round(Math.random()*1E5));
		HashMapping mapping=new HashMapping(context, hMap);
		//mapping.setOpID(opID);
		return mapping;
			
	}
	
	public static void send(Mapping mapping){
		provider.getEPRuntime().sendEvent(mapping);
	}
	
	public static class Subscriber{
		
		long count=0;
		EPStatement stm;
		public Subscriber(EPStatement stm){
			this.stm=stm;
		}
		
		public void update(Mapping mapping){
		
			new Thread(new Runnable() {
				
				public void run() {
					// TODO Auto-generated method stub
					SafeIterator<EventBean> itr=stm.safeIterator();
					for(;itr.hasNext();){
						Mapping map=(Mapping)itr.next().getUnderlying();
						count++;
					}
					itr.close();
				}
			}).start();
			//count++;
		}
		
		public long count(){
			return count;
		}
		
	}
}
