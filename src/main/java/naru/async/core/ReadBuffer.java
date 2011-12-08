package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class ReadBuffer implements BufferGetter {
	private static Logger logger=Logger.getLogger(ReadBuffer.class);
	private static final String STORE_CREANUP="storeCreanup";
	
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private ChannelContext context;
	//setupで設定されrecycleされるまで保持する
	private boolean isContextUnref=false;
	
	private Store store;
	private long onBufferLength;
	
	/* 0長を受信した場合は、今までputBufferされた全bufferを返却した後、
	 * その次のasyncReadをonCloseで復帰させる。*/
	private boolean isDisconnect=false;//回線が切れた場合
	
	private synchronized void setStore(Store store){
		if(store!=null){
			context.ref();
			store.ref();
		}
		Store orgStore=this.store;
		this.store=store;
		if(orgStore!=null){
			context.unref();
			orgStore.unref();
		}
	}
	
	public ReadBuffer(ChannelContext context){
		this.context=context;
	}
	
	public void dump(){
		dump(logger);
	}
	public void dump(Logger logger){
		try {
			logger.debug("$realReadLength:"+store.getPutLength() + ":callbackLength:"+store.getGetLength());
			logger.debug("$workBuffer:"+workBuffer);
		} catch (RuntimeException e) {
			//workBufferをtoStringする時にConcurrentModificationExceptionが発生する場合がある
		}
	}
	
	public void recycle() {
		Iterator<ByteBuffer> itr=workBuffer.iterator();
		while(itr.hasNext()){
			ByteBuffer buf=itr.next();
			PoolManager.poolBufferInstance(buf);
			itr.remove();
		}
		if(store!=null){
			store.unref();
			store=null;
		}
	}
	
	public void setup(){
		onBufferLength=0;
		isDisconnect=false;
		logger.debug("setup().cid:"+context.getPoolId()+":workBuffer:"+workBuffer);
		store=Store.open(false);//storeはここでしか設定しない
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		isContextUnref=false;
//		setStore(Store.open(false));
	}
	
	public void cleanup(){
		logger.debug("cleanup.cid:"+context.getPoolId());
		if(store!=null){
			store.close(this,store);
//			setStore(null);
		}
	}
	
	//回線からreadしたbufferは、putBuffersメソッドで詰め込む
	public void putBuffer(ByteBuffer[] buffer){
		if(store==null || store.isCloseReceived()){
			logger.warn("store closed.cid:"+context.getPoolId()+":store:"+store);
			PoolManager.poolBufferInstance(buffer);
			return;
		}
		logger.debug("pubBuffer.cid:"+context.getPoolId()+":bufSize:"+BuffersUtil.remaining(buffer));
		store.putBuffer(buffer);
	}
	
	public void disconnect(){
		logger.debug("disconnect.cid:"+context.getPoolId());
		isDisconnect=true;
	}
	
	//以下条件がそろうとcallbackする。
	//1)回線から読み込んだbufferがある。
	//2)read Orderがある
	//ChannelContextのioLockの中から呼び出されるため２重に走行する事はない。
	public boolean callback(/*ContextOrders orders*/){
		logger.debug("callback.cid:"+context.getPoolId()+":isDisconnect:"+isDisconnect);
		boolean doneDisconnect=false;
		synchronized(workBuffer){//workBufferを守る
			int size=workBuffer.size();
			if(size!=0){
				ByteBuffer[] buffer=(ByteBuffer[])workBuffer.toArray(BuffersUtil.newByteBufferArray(size));
				long bufSize=BuffersUtil.remaining(buffer);
				if(context.ordersDoneRead(buffer)){
					logger.debug("callback.ordersDoneRead.cid:" + context.getPoolId()+ ":bufSize:" + bufSize +":hashCode:"+buffer.hashCode());
					workBuffer.clear();
					return true;
				}
				PoolManager.poolArrayInstance(buffer);//配列を返却
				//bufferはあるけどasyncReadリクエストがない
				if(isDisconnect){
					logger.debug("isDisconnect and not asyncRead.cid:"+context.getPoolId()+":"+buffer.length);
					//現在callback中だけどまたasyncReadしていないタイミングで回線が切れる事がある
					//doneDisconnect=true;
				}
//				return false;
			}else if(store==null||(store.getPutLength()==onBufferLength && isDisconnect)){
				//回線が切れている、かつ受信したすべてのデータを通知した
				logger.debug("doneDisconnect.cid:"+context.getPoolId());
				doneDisconnect=true;
			}else if(isDisconnect){
				//回線は切断されているが、Storeにまだデータが残っている
				logger.debug("isDisconnect but not doneDisconnect.cid:"+context.getPoolId()+ ":store.getPutLength():"+store.getPutLength() +":onBufferLength:" +onBufferLength);
//				return store.asyncBuffer(this, store);
			}
		}
		if(doneDisconnect){
			context.doneClosed(false);
			return true;
		}
		return store.asyncBuffer(this, store);
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffer) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			return false;
		}
//		ContextOrders orders=(ContextOrders)userContext;
		boolean	doneDisconnect=false;
		synchronized(workBuffer){//workBufferを守る
			onBufferLength+=BuffersUtil.remaining(buffer);
			for(int i=0;i<buffer.length;i++){
				workBuffer.add(buffer[i]);
			}
			//配列を返却
			PoolManager.poolArrayInstance(buffer);
			int size=workBuffer.size();
			ByteBuffer[] readBuffer=(ByteBuffer[])workBuffer.toArray(BuffersUtil.newByteBufferArray(size));
			long bufSize=BuffersUtil.remaining(readBuffer);
			if(context.ordersDoneRead(readBuffer)){
				workBuffer.clear();
				logger.debug("onBuffer ordersDoneRead return true cid:"+ context.getPoolId()+ ":store:"+store+":bufSize:"+bufSize+":size:"+workBuffer.size());
			}else{
				PoolManager.poolArrayInstance(readBuffer);//配列を返却
				logger.debug("onBuffer return false cid:"+ context.getPoolId()+ ":store:"+store+":buffer:"+BuffersUtil.remaining(buffer)+":size:"+workBuffer.size());
				return false;
			}
			/* callbackしたが回線が切れていた場合、doneClosedも実施する必要がある */
			if(store==null||(store.getPutLength()==onBufferLength && isDisconnect)){
				//回線が切れている、かつ受信したすべてのデータを通知した
				logger.debug("doneDisconnect.cid:"+context.getPoolId());
				doneDisconnect=true;
			}else if(isDisconnect){
				//回線は切断されているが、Storeにまだデータが残っている
				logger.debug("isDisconnect but not doneDisconnect.cid:"+context.getPoolId()+ ":store.getPutLength():"+store.getPutLength() +":onBufferLength:" +onBufferLength);
			}
		}
		if(doneDisconnect){//storeがnullの場合、必ずここdoneDisconnect=true
			context.doneClosed(false);
			return true;
		}
		return store.asyncBuffer(this, store);
	}
	
	public void onBufferEnd(Object userContext) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			return;
		}
		synchronized(this){
			if(isContextUnref){
				logger.error("duplicate ReadBuffer#onBufferEnd",new Throwable());
			}
			isContextUnref=true;
//			ContextOrders orders=(ContextOrders)userContext;
			logger.debug("onBufferEnd.cid:"+context.getPoolId());//こないと思う
//			setStore(null);
			context.unref();//storeが終了したのでcontextは開放してもよい
		}
	}
	
	public void onBufferFailure(Object userContext, Throwable falure) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			return;
		}
		synchronized(this){
			if(isContextUnref){
				logger.error("duplicate ReadBuffer#onBufferFailure",new Throwable());
			}
			isContextUnref=true;
//			ContextOrders orders=(ContextOrders)userContext;
			logger.warn("onBufferFailure falure",falure);//こないと思う
			logger.warn("onBufferFailure now",new Exception());
			context.failure(falure);
			context.unref();//storeが終了したのでcontextは開放してもよい
//			setStore(null);
		}
	}

	public long getOnBufferLength() {
		return onBufferLength;
	}
}
