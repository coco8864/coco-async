package naru.async.pool;

import java.util.LinkedList;

import org.apache.log4j.Logger;

public class LocalPool {
	private static Logger logger=Logger.getLogger(LocalPool.class);
	public LocalPool(Pool pool) {
		this.pool=pool;
	}
	Pool pool;
	LinkedList<Object> freePool=new LinkedList<Object>();
	LinkedList<Object> freeRsvPool=new LinkedList<Object>();
	LinkedList<Object> usedPool=new LinkedList<Object>();
	boolean isChargePool;
	int getCount;
	int poolCount;
	int hit;
	int max;
	int total;
	void beat(){
		pool.batchPool(usedPool);
		pool.batchGet(freePool,max);
		if(max<getCount){
			max=getCount;
		}
		hit=getCount=poolCount=0;
	}
	void term(){
		if(max!=0){
			logger.info(pool.getDispname()+":total:" +total +":max:"+max);
		}
		pool.batchPool(freePool);
		pool.batchPool(freeRsvPool);
		pool.batchPool(usedPool);
	}
	
	Object get(){
		getCount++;
		total++;
		if(freePool.size()==0){
			if(!isChargePool){
				return null;
			}
			synchronized(this){
				if(freeRsvPool.size()==0){
					return null;
				}
				LinkedList<Object> tmp=freeRsvPool;
				freeRsvPool=freePool;
				freePool=tmp;
				PoolManager.enqueuePool(this);
			}
		}
		hit++;
		return freePool.removeFirst();
	}
	
	synchronized void chargeRsvPool(){
		pool.batchGet(freeRsvPool,max);
	}
	public void setupChargePool(int max) {
		this.max=max;
		isChargePool=true;
		pool.batchGet(freePool,max);
		pool.batchGet(freeRsvPool,max);
	}
}
