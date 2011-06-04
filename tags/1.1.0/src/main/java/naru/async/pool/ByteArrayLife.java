package naru.async.pool;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;

public class ByteArrayLife extends ReferenceLife {
	static private Logger logger=Logger.getLogger(ByteArrayLife.class);
	
	private byte[] array;
	private Set<ByteBufferLife> byteBufferLifes=new HashSet<ByteBufferLife>();//同一seedの次ReferenceLife
	
	public ByteArrayLife(ByteBuffer firstByteBuffer,Pool pool) {
		super(firstByteBuffer.array());
		setPool(pool);
		array=firstByteBuffer.array();
		ByteBufferLife byteBufferLife=new ByteBufferLife(firstByteBuffer,this);
//		synchronized(byteBufferLifes){//作った直後なので参照している人がいないはず
			byteBufferLifes.add(byteBufferLife);
//		}
	}

	void gcInstance() {//自分がarrayへの参照を保持しているので呼ばれるわけがない
	}

	/*　getBufferInstanceの延長で呼び出される事を想定 */
	/* 唯一持っているはずのByteBufferを返却,PoolからとられたばかりだからByteBufferは、1個のはず */
	ByteBuffer getOnlyByteBuffer(){
		synchronized(byteBufferLifes){
//			logger.debug("getOnlyByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(byteBufferLifes.size()!=1){
				throw new IllegalStateException("byteBufferLifes.size()="+byteBufferLifes.size());
			}
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			ByteBufferLife byteBufferLife=itr.next();
			byteBufferLife.ref();
			ByteBuffer byteBuffer=(ByteBuffer)byteBufferLife.get();
			ref();
			return byteBuffer;
		}
	}
	
	/*　getBufferInstanceの延長で呼び出される事を想定 */
	/* 唯一持っているはずのByteBufferを返却,PoolからとられたばかりだからByteBufferは、1個のはず */
	ByteBufferLife getOnlyByteBufferLife(){
		synchronized(byteBufferLifes){
			logger.debug("getOnlyByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(byteBufferLifes.size()!=1){
				throw new IllegalStateException("byteBufferLifes.size()="+byteBufferLifes.size());
			}
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			ByteBufferLife byteBufferLife=itr.next();
			return byteBufferLife;
		}
	}
	
	
	/*　duplicateの延長で呼び出される事を想定 */
	ByteBuffer getByteBuffer(){
		ByteBuffer byteBuffer=ByteBuffer.wrap(array);
		ByteBufferLife byteBufferLife=new ByteBufferLife(byteBuffer,this);
		byteBufferLife.ref();
		synchronized(byteBufferLifes){
			logger.debug("getByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			byteBufferLifes.add(byteBufferLife);
			ref();
		}
		return byteBuffer;
	}
	
	/* poolBufferInstanceの延長で呼び出される事を想定 */
	void poolByteBuffer(ByteBuffer buffer){
		synchronized(byteBufferLifes){
//			logger.debug("poolByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(unref()){
				if(byteBufferLifes.size()!=1){
					throw new IllegalStateException("byteBufferLifes.size()="+byteBufferLifes.size());
				}
				Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
				ByteBufferLife byteBufferLife=itr.next();
				byteBufferLife.unref();
				pool.poolInstance(buffer);
				return;
			}
			if(getRef()==0){//pool中にいる
				//2重poolBufferInstance()..
				logger.error("poolByteBuffer...getRef()==0",new Throwable());//TODO
				return;
			}
			ByteBufferLife byteBufferLife=removeByteBuffer(buffer);
			if(byteBufferLife==null){
				//2重poolBufferInstance()..
				logger.error("poolByteBuffer...",new Throwable());//TODO
				return;
			}
			byteBufferLife.unref();
			byteBufferLife.clear();
		}
	}
	
	private ByteBufferLife 	removeByteBuffer(ByteBuffer buffer){
		synchronized(byteBufferLifes){
			logger.debug("removeByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			
			Iterator<ByteBufferLife> itr=byteBufferLifes.iterator();
			while(itr.hasNext()){
				ByteBufferLife life=itr.next();
				if( life.get()==buffer){
					itr.remove();
					return life;
				}
			}
		}
		return null;
	}
	
	/* gcの延長で呼び出される事を想定 */
	void gcByteBufferLife(ByteBufferLife byteBufferLife){
		logger.warn("gcByteBufferLife.getInstance date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet,stackOfGet);
		pool.gcLife(byteBufferLife);
		synchronized(byteBufferLifes){
			if( byteBufferLifes.remove(byteBufferLife)==false ){
				throw new IllegalStateException();
			}
			byteBufferLife.unref();
			byteBufferLife.clear();
			if(unref()){
				//最後のByteBufferLifeがGCされた場合は、・・・
				ByteBuffer byteBuffer=ByteBuffer.wrap(array);
				byteBufferLife=new ByteBufferLife(byteBuffer,this);
				byteBufferLife.ref();
				synchronized(byteBufferLifes){
					byteBufferLifes.add(byteBufferLife);
				}
				pool.poolInstance(byteBuffer);
				return;
			}
		}
	}
	
	public void info(){
		info(false);
	}
	
	public void info(boolean isDetail){
		Object[] bfls=byteBufferLifes.toArray();
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
}
