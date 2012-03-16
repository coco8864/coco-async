package naru.async.pool;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import naru.async.pool.ByteBufferLife.SearchKey;

import org.apache.log4j.Logger;

public class ByteArrayLife extends ReferenceLife {
	static private Logger logger=Logger.getLogger(ByteArrayLife.class);
	
	private byte[] array;
	
	/*ByteBufferをキーにByteBufferLifeを検索する時に使う */
	private SearchKey searchKey=new SearchKey();//userLifesのロック中に利用
	//同一seedの次ReferenceLife
	private Set<ByteBufferLife> userLifes=new HashSet<ByteBufferLife>();
	//freeLifesが管理するByteBufferがGCされないようにvalueで参照しておく
	private Map<ByteBufferLife,ByteBuffer> freeLifes=new HashMap<ByteBufferLife,ByteBuffer>();
	
	public ByteArrayLife(ByteBuffer firstByteBuffer,Pool pool) {
		super(firstByteBuffer.array());
		setPool(pool);
		array=firstByteBuffer.array();
		ByteBufferLife byteBufferLife=new ByteBufferLife(firstByteBuffer,this);
		freeLifes.put(byteBufferLife,firstByteBuffer);
//		userLifes.add(byteBufferLife);
	}

	void gcInstance() {//自分がarrayへの参照を保持しているので呼ばれるわけがない
	}

	/*　初回getBufferInstanceの延長で呼び出される事を想定 */
	/* 唯一持っているはずのByteBufferを返却,PoolからとられたばかりだからByteBufferは、1個のはず */
	ByteBuffer getOnlyByteBuffer(ByteBuffer byteBuffer){
		synchronized(userLifes){
//			logger.debug("getOnlyByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(freeLifes.size()<1){
				throw new IllegalStateException("freeLifes.size()="+freeLifes.size());
			}
			searchKey.setByteBuffer(byteBuffer);
			if(byteBuffer!=freeLifes.remove(searchKey)){
				throw new IllegalStateException("freeLifes.size()="+freeLifes.size());
			}
			ByteBufferLife byteBufferLife=searchKey.getHit();
//			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
//			ByteBufferLife byteBufferLife=itr.next();
//			itr.remove();
			userLifes.add(byteBufferLife);
			byteBufferLife.ref();
//			ByteBuffer byteBuffer=(ByteBuffer)byteBufferLife.get();
			ref();
			return byteBuffer;
		}
	}
	
	/*　最終poolInstanceの延長で呼び出される事を想定 */
	/* 唯一持っているはずのByteBufferを返却,PoolからとられたばかりだからByteBufferは、1個のはず */
	ByteBufferLife getOnlyByteBufferLife(){
		synchronized(userLifes){
//			logger.debug("getOnlyByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
			if(userLifes.size()!=1){
				throw new IllegalStateException("byteBufferLifes.size()="+userLifes.size());
			}
			Iterator<ByteBufferLife> itr=userLifes.iterator();
			ByteBufferLife byteBufferLife=itr.next();
			itr.remove();
			byteBufferLife.unref();
			freeLifes.put(byteBufferLife,(ByteBuffer)byteBufferLife.get());
			return byteBufferLife;
		}
	}
	
	/*　duplicateの延長で呼び出される事を想定 */
	ByteBuffer getByteBuffer(){
		synchronized(userLifes){
			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
			if(itr.hasNext()){
				ByteBufferLife life=itr.next();
				itr.remove();
				userLifes.add(life);
				life.ref();
				ByteBuffer byteBuffer=(ByteBuffer)life.get();
				ref();
				return byteBuffer;
			}
			ByteBuffer byteBuffer=ByteBuffer.wrap(array);
			ByteBufferLife byteBufferLife=new ByteBufferLife(byteBuffer,this);
			byteBufferLife.ref();
//			logger.debug("getByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
			if(userLifes.add(byteBufferLife)==false){
				logger.error("byteBufferLifes.add return false",new Throwable());
			}
			ref();
			return byteBuffer;
		}
	}
	
	/* poolBufferInstanceの延長で呼び出される事を想定 */
	void poolByteBuffer(ByteBuffer buffer){
		synchronized(userLifes){
			ByteBufferLife byteBufferLife=removeByteBuffer(buffer);
			if(byteBufferLife==null){
				//2重poolBufferInstance()..
				logger.error("poolByteBuffer...",new Throwable());//TODO
				return;
			}
			byteBufferLife.unref();
			if(freeLifes.size()<16){
				freeLifes.put(byteBufferLife,buffer);
			}else{
				/* poolに十分あるのでこのインスタンスは捨てる */
				byteBufferLife.clear();//clearしてもqueueされることがある...なぜ???
			}
			if(unref()){
				if(userLifes.size()!=0){
					throw new IllegalStateException("byteBufferLifes.size()="+userLifes.size());
				}
				pool.poolInstance(buffer);//ArrayLifeがpoolに帰るので、一つだけByteBufferをpoolに戻す
				return;
			}
			if(getRef()==0){//pool中にいる
				//2重poolBufferInstance()..
				logger.error("poolByteBuffer...getRef()==0",new Throwable());//TODO
				return;
			}
		}
	}
	
	private ByteBufferLife 	removeByteBuffer(ByteBuffer buffer){
		logger.debug("removeByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
		synchronized(userLifes){
			searchKey.setByteBuffer(buffer);
			if(userLifes.remove(searchKey)){
				return searchKey.getHit();
			}
			/*
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			while(itr.hasNext()){
				ByteBufferLife life=itr.next();
				if( life.get()==buffer){
					itr.remove();
					return life;
				}
			}
			*/
		}
		return null;
	}
	
	/* gcの延長で呼び出される事を想定 */
	void gcByteBufferLife(ByteBufferLife byteBufferLife){
		if(byteBufferLife.getRef()==0){
			//clearしてもReferenceQueueに通知される事がある
			return;
		}
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet+":BBLsize:"+userLifes.size()+":byteBufferLife:"+byteBufferLife,stackOfGet);
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(byteBufferLife.timeOfGet))+":get thread:"+byteBufferLife.threadNameOfGet);
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(byteBufferLife.timeOfPool))+":pool thread:"+byteBufferLife.threadNameOfPool,byteBufferLife.stackOfPool);
		logger.warn("gcByteBufferLife.getInstance get:"+byteBufferLife.get());
		
		synchronized(userLifes){
			if( userLifes.remove(byteBufferLife)==false ){
				throw new IllegalStateException();
			}
			byteBufferLife.unref();
			byteBufferLife.clear();
			if(unref()){
				/* ArrayLifeがpoolに帰るので,返却するByteBufferを作る */
				ByteBuffer byteBuffer=ByteBuffer.wrap(array);
				byteBufferLife=new ByteBufferLife(byteBuffer,this);
				pool.poolInstance(byteBuffer);//ByteBufferをpoolに戻す
			}
		}
	}
	
	public void info(){
		info(false);
	}
	
	public void info(boolean isDetail){
		Object[] bfls=userLifes.toArray();
//		Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
		logger.debug("array:"+array +":ref:"+ getRef());
		for(int i=0;i<bfls.length;i++){
			ByteBufferLife bbl=(ByteBufferLife)bfls[i];
			if(isDetail){
				logger.debug("referent:"+bbl.get() +":refCount:"+bbl.getRef()+":getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+bbl.threadNameOfGet,bbl.stackOfGet);
			}else{
				logger.debug("referent:"+bbl.get() +":refCount:"+bbl.getRef()+":getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+bbl.threadNameOfGet/*,stackOfGet*/);
			}
		}
	}
	
	@Override
	public String toString(){
		return "$$ByteArrayLife." +array.length;
	}
}
