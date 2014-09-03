package naru.async.pool;

import java.lang.ref.WeakReference;
import java.text.SimpleDateFormat;
import java.util.Date;

import naru.async.ChannelHandler;
import naru.async.Log;
import naru.async.core.ChannelContext;

import org.apache.log4j.Logger;

public class ReferenceLife extends WeakReference {
	private static Logger logger=Logger.getLogger(ReferenceLife.class);
	private static SimpleDateFormat logDateFormat=null; 
	static{
		logDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
	}
	public static String fomatLogDate(Date date){
		synchronized(logDateFormat){
			return logDateFormat.format(date);
		}
	}
	
	protected Pool pool;
	private String poolClassName=null;
	
	protected Throwable stackOfGet;
	protected long timeOfGet;
	protected String threadNameOfGet;
	protected Throwable stackOfPool;
	protected long timeOfPool;
	protected String threadNameOfPool;
	protected int countOfGet=0;
	protected int countOfPool=0;
	protected volatile int refCounter=0;//参照数(0の状態でのみpoolInstance可能
	
	public ReferenceLife(Object referent) {
		super(referent,PoolManager.getReferenceQueue());
	}
	
	void setPool(Pool pool){
		this.pool=pool;
		this.poolClassName=pool.getPoolClass().getName();
	}
	Pool getPool(){
		return pool;
	}
	
	//referentがgcされてしまった。poolInstanceが漏れていると推測される
	void gcInstance(){
		logger.warn("gcInstance.getInstance poolClassName:"+poolClassName+":date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet,stackOfGet);
		pool.gcLife(this);
	}
	
	int getRef(){
		return refCounter;
	}
	
	private long referentId;
	synchronized void ref(){
		if(refCounter==0){
			timeOfGet=System.currentTimeMillis();
			if(logger.isDebugEnabled()){
				stackOfGet=new Throwable(poolClassName);
			}
			Object o=get();
			if(o instanceof PoolBase){
				referentId=((PoolBase)o).getPoolId();
			}else if(o!=null){
				referentId=0;//o.hashCode();
			}else{
				referentId=0;
			}
			threadNameOfGet=Thread.currentThread().getName();
			countOfGet++;
		}
		refCounter++;
		if(pool!=null){
			pool.lifeInfo(referentId, refCounter, true);
		}
	}
	
	synchronized boolean unref(){
		if(pool!=null){
			pool.lifeInfo(referentId, refCounter, false);
		}
		if(refCounter==0){
			//2重開放
			logger.error("unref duplicate pool poolClassName:"+poolClassName +":"+System.identityHashCode(get()));
			logger.error("getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet,stackOfGet);
			logger.error("poolInstance date:"+fomatLogDate(new Date(timeOfPool))+":thread:"+threadNameOfPool,stackOfPool);
			logger.error("this call date:"+fomatLogDate(new Date()),new Throwable(poolClassName));
			return false;//エラーpoolに入れてはいけない
		}
		refCounter--;
		if(refCounter==0){
			timeOfPool=System.currentTimeMillis();
			if(logger.isDebugEnabled()){
				stackOfPool=new Throwable();
			}
			threadNameOfPool=Thread.currentThread().getName();
			countOfPool++;
			return true;//pool this
		}
		return false;//using not yet pool this
	}
	
	public void info(){
		info(false);
	}
	
	public void info(boolean isDetail){
		if(isDetail){
			Log.debug(logger,"referent:",get(),":refCount:",getRef(),":getInstance date:",fomatLogDate(new Date(timeOfGet)),":thread:",threadNameOfGet,stackOfGet);
			if(pool.getPoolClass()==ChannelHandler.class){
				ChannelHandler handler=(ChannelHandler) get();
				if(handler!=null){
					handler.dump();
				}
			}else if(pool.getPoolClass()==ChannelContext.class){
				ChannelContext context=(ChannelContext) get();
				if(context!=null){
					context.dump();
				}
			}
		}else{
			Log.debug(logger,"referent:",get(),":refCount:",getRef(),":getInstance date:",fomatLogDate(new Date(timeOfGet)),":thread:",threadNameOfGet/*,stackOfGet*/);
		}
	}
	
}
