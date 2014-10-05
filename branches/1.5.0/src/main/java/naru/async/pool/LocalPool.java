package naru.async.pool;

import java.util.LinkedList;

import naru.async.Log;

import org.apache.log4j.Logger;

/*
 * sample timeに来る獲得要求数をカウント maxとしてpool数にする
 */
public class LocalPool {
	private static Logger logger=Logger.getLogger(LocalPool.class);
	//private static int SAMPLE_TIME=1000;
	public LocalPool(Pool pool) {
		this.pool=pool;
		this.max=1;
		charge();
		//PoolManager.enqueuePool(this);
	}
	private Pool pool;
	private LinkedList<Object> freePool=new LinkedList<Object>();
	private LinkedList<Object> spareFreePool=new LinkedList<Object>();
	private long sampleStartTime=0;
	private long maxTime=0;
	private int getCount=0;
	private int chargeCount=0;
	private int max;
	private int miss=0;
	private int total=0;
	
	void term(){
		if(max!=0){
			logger.info(pool.getDispname()+":total:" +total +":miss:"+miss+":chargeCount:"+chargeCount+":max:"+max);
		}
		pool.batchPool(freePool);
		pool.batchPool(spareFreePool);
	}
	
	Object get(){
		long now=System.currentTimeMillis();
		if(now>(sampleStartTime+PoolManager.getLocalPoolInterval())){
			getCount=0;
			sampleStartTime=now;
		}else{
			if(getCount>max){
				max=getCount;
				maxTime=sampleStartTime;
			}
		}
		getCount++;
		total++;
		if(freePool.size()==0){
			synchronized(this){
				if(spareFreePool.size()==0){
					miss++;
					return null;
				}
				chargeCount++;
				LinkedList<Object> tmp=spareFreePool;
				spareFreePool=freePool;
				freePool=tmp;
				PoolManager.enqueuePool(this);
			}
		}
		return freePool.removeFirst();
	}
	
	synchronized void charge(){
//		logger.info(pool.getDispname()+":charge");
		pool.charge(spareFreePool,max);
	}
	
	public void info() {
		if(max!=0){
			logger.info(pool.getDispname()+":total:" +total+":chargeCount:" +chargeCount +":miss:"+miss+":max:"+max);
		}
	}
}
