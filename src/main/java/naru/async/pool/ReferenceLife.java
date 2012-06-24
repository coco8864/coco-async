package naru.async.pool;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.text.SimpleDateFormat;
import java.util.Date;

import naru.async.ChannelHandler;
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
	protected volatile int refCounter=0;//�Q�Ɛ�(0�̏�Ԃł̂�poolInstance�\

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
	
	//referent��gc����Ă��܂����BpoolInstance���R��Ă���Ɛ��������
	void gcInstance(){
		logger.warn("gcInstance.getInstance poolClassName:"+poolClassName+":date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet,stackOfGet);
		pool.gcLife(this);
	}
	
	int getRef(){
		return refCounter;
	}
	
	synchronized void ref(){
		/*����N���X�̃��C�t�T�C�N��������ꍇ
		if(pool!=null && pool.getPoolClass()==FileSystemHandler.class){
			logger.debug("ref instance:"+get(),new Throwable());
		}
		*/
		if(refCounter==0){
			/*�ȉ����������Ȃ����́A���肦�Ȃ��̂Ń`�F�b�N���Ȃ�
			if(countOfGet==countOfPool){
				//error
			}
			*/
			timeOfGet=System.currentTimeMillis();
			if(logger.isDebugEnabled()){
				stackOfGet=new Throwable(poolClassName);
			}
			threadNameOfGet=Thread.currentThread().getName();
			countOfGet++;
		}
		refCounter++;
//		if("naru.async.store2.Store".equals(poolClassName)){
//			new Throwable("ref:"+ this + ":refCounter:"+refCounter).printStackTrace();
//		}
	}
	
	synchronized boolean unref(){
		/*����N���X�̃��C�t�T�C�N��������ꍇ
		if(pool!=null &&pool.getPoolClass()==FileSystemHandler.class){
			logger.debug("unref instance:"+get(),new Throwable());
		}
		*/
		if(refCounter==0){
			//2�d�J��
			logger.error("unref duplicate pool poolClassName:"+poolClassName);
			logger.error("getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet,stackOfGet);
			logger.error("poolInstance date:"+fomatLogDate(new Date(timeOfPool))+":thread:"+threadNameOfPool,stackOfPool);
			logger.error("this call date:"+fomatLogDate(new Date()),new Throwable(poolClassName));
			return false;//�G���[pool�ɓ���Ă͂����Ȃ�
		}
		refCounter--;
//		if("naru.async.store2.Store".equals(poolClassName)){
//			new Throwable("unref:"+ this + ":refCounter:"+refCounter).printStackTrace();
//		}
		if(refCounter==0){
			timeOfPool=System.currentTimeMillis();
			if(logger.isDebugEnabled()){
				stackOfPool=new Throwable();
			}
			threadNameOfPool=Thread.currentThread().getName();
			countOfPool++;
			return true;//pool�ɓ���Ă悢
		}
		return false;//�܂��g�p���Apool�ɓ���Ă͂����Ȃ�
	}
	
	public void info(){
		info(false);
	}
	
	public void info(boolean isDetail){
		if(isDetail){
			logger.debug("referent:"+get() +":refCount:"+getRef()+":getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet,stackOfGet);
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
			logger.debug("referent:"+get() +":refCount:"+getRef()+":getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet/*,stackOfGet*/);
		}
	}
	
}
