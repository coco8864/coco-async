package naru.async.pool;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Context extends PoolBase {
	protected Map attribute=new HashMap();
	
	@Override
	public void recycle(){
		synchronized(attribute){
			Iterator itr=attribute.values().iterator();
			while(itr.hasNext()){
				Object value=itr.next();
				if(value instanceof PoolBase){
					((PoolBase)(value)).unref();
				}
			}
			attribute.clear();
		}
		super.recycle();
	}
	
	public void endowAttribute(String name,PoolBase value){
		setAttribute(name,value);
		if(value!=null){
			value.unref();
		}
	}
	
	public void setAttribute(String name,Object value){
		Object orgValue=null;
		if(value!=null && value instanceof PoolBase){
			((PoolBase)(value)).ref();
		}
		synchronized(attribute){
			orgValue=attribute.remove(name);
			attribute.put(name, value);
		}
		if(orgValue!=null && orgValue instanceof PoolBase){
			((PoolBase)(orgValue)).unref();
		}
	}
	
	public Object getAttribute(String name){
		return attribute.get(name);
	}
	
	public Object removeAttribute(String name){
		Object orgValue=null;
		synchronized(attribute){
			orgValue=attribute.remove(name);
		}
		if(orgValue!=null && orgValue instanceof PoolBase){
			((PoolBase)(orgValue)).unref();
		}
		return orgValue;
	}

}
