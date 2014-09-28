package naru.async.pool;

import java.util.LinkedList;

import org.apache.log4j.Logger;

public class LocalPool {
	private static Logger logger=Logger.getLogger(LocalPool.class);
	public LocalPool(Pool pool) {
		this.pool=pool;
	}
	private Pool pool;
	private LinkedList<Object> freePool=new LinkedList<Object>();
	private LinkedList<Object> spareFreePool=new LinkedList<Object>();
	private LinkedList<Object> usedPool=new LinkedList<Object>();
	private LinkedList<Object> spareUsedPool=new LinkedList<Object>();
	private boolean isAutoCharge=false;
	private boolean isCharge=true;
	private int getCount;
	private int poolCount;
	private int max;
	private int hit;
	private int total;
	void pause(){
		if(getCount==0&&poolCount==0){
			return;
		}
		if(max<getCount){
			max=getCount;
		}
		getCount=poolCount=0;
		synchronized(this){
			while(isCharge==false){
				//try {
				//	wait();
				//} catch (InterruptedException e) {
				//}
				pool.batchPool(spareUsedPool);
				pool.batchGet(spareFreePool,max);
				isCharge=true;
			}
			LinkedList<Object> tmp=spareFreePool;
			spareFreePool=freePool;
			freePool=tmp;
			tmp=spareUsedPool;
			spareUsedPool=usedPool;
			usedPool=tmp;
			//pool.batchPool(spareUsedPool);
			//pool.batchGet(spareFreePool,max);
			//isCharge=true;
			isCharge=false;
			PoolManager.enqueuePool(this);
		}
	}
	void term(){
		if(max!=0){
			logger.info(pool.getDispname()+":total:" +total +":hit:"+hit+":max:"+max);
		}
		pool.batchPool(freePool);
		pool.batchPool(spareFreePool);
		pool.batchPool(usedPool);
		pool.batchPool(spareUsedPool);
	}
	
	void pool(Object obj){
		poolCount++;
		usedPool.add(obj);
	}
	
	Object get(){
		getCount++;
		total++;
		if(freePool.size()==0){
			if(!isAutoCharge){
				return null;
			}
			synchronized(this){
				if(spareFreePool.size()==0){
					return null;
				}
				LinkedList<Object> tmp=spareFreePool;
				spareFreePool=freePool;
				freePool=tmp;
				isCharge=false;
				PoolManager.enqueuePool(this);
			}
			logger.info(pool.getDispname()+":autoCharge request");
		}
		hit++;
		return freePool.removeFirst();
	}
	
	synchronized void charge(){
		if(isCharge){
			return;
		}
//		logger.info(pool.getDispname()+":charge");
		pool.batchPool(spareUsedPool);
		pool.batchGet(spareFreePool,max);
		isCharge=true;
		notify();
	}
	
	public void setupAutoChargePool(int max) {
		this.max=max;
		isAutoCharge=true;
		pool.batchGet(freePool,max);
		pool.batchGet(spareFreePool,max);
	}
	
	public void info() {
		if(max!=0){
			logger.info(pool.getDispname()+":total:" +total +":hit:"+hit+":max:"+max);
		}
	}
}
