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
	private static Logger logger=Logger.getLogger(ByteArrayLife.class);
	private static final int FREE_LIFE_MAX = 16;
	private byte[] array;
	
	/*ByteBufferをキーにByteBufferLifeを検索する時に使う */
	private SearchKey searchKey=new SearchKey();//userLifesのロック中に利用
	//同一seedの次ReferenceLife
	private Set<ByteBufferLife> useLifes=new HashSet<ByteBufferLife>();
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
	ByteBuffer getFirstByteBuffer(ByteBuffer byteBuffer){
		synchronized(useLifes){
//			logger.debug("getOnlyByteBuffer:size:" +byteBufferLifes.size() +":refCounter:"+refCounter);
			if(freeLifes.size()<1){
				logger.error("fail to getFirstByteBuffer no freeLifes",new IllegalStateException());
				return null;//pool中が壊れていただけであり、呼び出し者は被害者、インスタンス取得を再試行
			}
			searchKey.setByteBuffer(byteBuffer);
			ByteBuffer wk=freeLifes.remove(searchKey);
			if(byteBuffer!=wk){
				logger.error("fail to getFirstByteBuffer not in freeLifes:"+freeLifes.size(),new IllegalStateException());
				return null;//pool中が壊れていただけであり、呼び出し者は被害者、インスタンス取得を再試行
			}
			ByteBufferLife byteBufferLife=searchKey.getHit();
//			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
//			ByteBufferLife byteBufferLife=itr.next();
//			itr.remove();
			useLifes.add(byteBufferLife);
			byteBufferLife.ref();
//			ByteBuffer byteBuffer=(ByteBuffer)byteBufferLife.get();
			ref();
			return byteBuffer;
		}
	}
	
	/*　poolから溢れて参照を開放する場合に呼び出される */
	void releaseLife(){
		synchronized(useLifes){
//			logger.debug("getOnlyByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
			if(useLifes.size()!=0){
				throw new IllegalStateException("byteBufferLifes.size()="+useLifes.size());
			}
			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
			while(itr.hasNext()){
				ByteBufferLife byteBufferLife=itr.next();
				itr.remove();
				byteBufferLife.clear();
			}
		}
	}
	
	/*　duplicateの延長で呼び出される事を想定 */
	ByteBuffer getByteBuffer(){
		synchronized(useLifes){
			Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
			while(itr.hasNext()){
				ByteBufferLife life=itr.next();
				ByteBuffer byteBuffer=(ByteBuffer)life.get();
				if(byteBuffer==null){
					logger.warn("getByteBuffer .freeLies.size():"+freeLifes.size()+":refCounter:"+refCounter);
					itr.remove();
					continue;
				}
				itr.remove();
				useLifes.add(life);
				life.ref();
				ref();
				return byteBuffer;
			}
			ByteBuffer byteBuffer=ByteBuffer.wrap(array);
			ByteBufferLife byteBufferLife=new ByteBufferLife(byteBuffer,this);
			byteBufferLife.ref();
//			logger.debug("getByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
			if(useLifes.add(byteBufferLife)==false){
				logger.error("byteBufferLifes.add return false",new Throwable());
			}
			ref();
			return byteBuffer;
		}
	}
	
	/* poolBufferInstanceの延長で呼び出される事を想定 */
	void poolByteBuffer(ByteBuffer buffer){
		synchronized(useLifes){
			ByteBufferLife byteBufferLife=removeByteBuffer(buffer);
			if(byteBufferLife==null){
				//2重poolBufferInstance()..
				logger.error("poolByteBuffer duplicate pool,useLies.size:"+useLifes.size()+":freeLifes.size:"+freeLifes.size(),new Exception());//TODO
				buffer.position(0);
				buffer.limit(128);
				logger.error(BuffersUtil.toStringFromBuffer(buffer, "utf-8"));
				Iterator<ByteBufferLife> itr=freeLifes.keySet().iterator();
				while(itr.hasNext()){
					ByteBufferLife l=itr.next();
					ByteBuffer b=freeLifes.get(l);
					if(b==buffer){
						logger.error("prev poolByteBuffer."+new Date(l.timeOfPool),l.stackOfPool);//TODO
					}
				}
				
				return;
			}
			byteBufferLife.unref();
			if(unref()){
				if(useLifes.size()!=0){
					logger.error("poolByteBuffer no useLifes,useLies.size:"+useLifes.size()+":freeLifes.size:"+freeLifes.size(),new Exception());//TODO
					return;
				}
				//代表のByteBufferをpoolに戻す
				//poolが溢れた場合この延長でreleaseLifeメソッドが呼び出される
				freeLifes.put(byteBufferLife,buffer);//場合によっては、FREE_LIFE_MAXを超える可能性あり
				pool.poolInstance(buffer);
				return;
			}
			if(freeLifes.size()<FREE_LIFE_MAX){
				freeLifes.put(byteBufferLife,buffer);
			}else{
				/* poolに十分あるのでこのインスタンスは捨てる */
				byteBufferLife.clear();//clearしてもqueueされることがある...なぜ???
			}
			if(getRef()==0){//pool中にいる
				//2重poolBufferInstance()..
				logger.error("poolByteBuffer...getRef()==0",new Exception());//TODO
				return;
			}
		}
	}
	
	private ByteBufferLife 	removeByteBuffer(ByteBuffer buffer){
//		logger.debug("removeByteBuffer:size:" +userLifes.size() +":refCounter:"+refCounter);
		synchronized(useLifes){
			searchKey.setByteBuffer(buffer);
			if(useLifes.remove(searchKey)){
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
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(timeOfGet))+":thread:"+threadNameOfGet+":BBLsize:"+useLifes.size()+":byteBufferLife:"+byteBufferLife,stackOfGet);
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(byteBufferLife.timeOfGet))+":get thread:"+byteBufferLife.threadNameOfGet);
		logger.warn("gcByteBufferLife.getInstance ByteBufferLife:date:"+fomatLogDate(new Date(byteBufferLife.timeOfPool))+":pool thread:"+byteBufferLife.threadNameOfPool,byteBufferLife.stackOfPool);
		logger.warn("gcByteBufferLife.getInstance get:"+byteBufferLife.get());
		
		synchronized(useLifes){
			if( useLifes.remove(byteBufferLife)==false ){
				throw new IllegalStateException();
			}
			byteBufferLife.unref();
			byteBufferLife.clear();
			if(unref()){
				/* 代表のByteBufferを作る */
				ByteBuffer byteBuffer=ByteBuffer.wrap(array);
				byteBufferLife=new ByteBufferLife(byteBuffer,this);
				freeLifes.put(byteBufferLife,byteBuffer);
				pool.poolInstance(byteBuffer);//ByteBufferをpoolに戻す
			}
		}
	}
	
	public void info(){
		info(false);
	}
	
	public void info(boolean isDetail){
		Object[] bfls=useLifes.toArray();
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
