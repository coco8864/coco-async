package naru.async.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolManager;
import naru.async.store.Store;

public class WriteBuffer implements BufferGetter {
	private static Logger logger=Logger.getLogger(WriteBuffer.class);
	private static final String STORE_CREANUP="storeCreanup";
//	private Object isBufReqLock=new Object();
//	private boolean isBufReq=false;
	
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private ChannelContext context;
	private Store store;
	private long onBufferLength;
//	private MessageDigest messageDigest;

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
	
	public WriteBuffer(ChannelContext context){
		this.context=context;
	}
	public void dump(){
		dump(logger);
	}
	
	public void dump(Logger logger){
		try {
			logger.debug("$aplWriteLength:"+store.getPutLength() + ":realWriteLength:"+store.getGetLength());
			logger.debug("$workBuffer.size:"+workBuffer.size());
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
		prepareWriteReturnNull=false;
	}
	
	public void setup(){
		logger.debug("setup().cid:"+context.getPoolId()+":workBuffer:"+workBuffer);
		onBufferLength=0;
		setStore(Store.open(false));
		store.asyncBuffer(this, store);
	}
	
	public void cleanup(){
		logger.debug("cleanup.cid:"+context.getPoolId());
		if(store!=null){
			store.close(this,store);
//			setStore(null);
		}
	}
	
	//アプリから貰ったbufferは、putBuffersで詰め込む
	public void putBuffer(ByteBuffer[] buffer){
		logger.debug("putBuffer cid:"+ context.getPoolId()+":store:"+store +":len:"+BuffersUtil.remaining(buffer));
		store.putBuffer(buffer);
		//write可能になるのを待つ
		/* 試しに以下をコメントアウト onBufferにあるので必要ないんじゃないか？ */
		//context.queueuSelect();
	}
	
	private boolean prepareWriteReturnNull=false;
	//queueIOする時にworkBufferがあることを確認しているので必ずworkBufferはある。
	public ByteBuffer[] prepareWrite(){
		logger.debug("prepareWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		ByteBuffer[] buffer=null;
		synchronized(workBuffer){
			int size=workBuffer.size();
			if(size!=0){
				buffer=(ByteBuffer[])workBuffer.toArray(BuffersUtil.newByteBufferArray(size));
//				workBuffer.clear();
			}else{
				prepareWriteReturnNull=true;
				logger.debug("prepareWrite size=0 cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
				//IOManagerがwriteしようとしたがバッファがまだstoreの中にあり準備できない。
				//そうばんonBufferが呼ばれるはず
			}
		}
		if(buffer==null){
			//２重になるかもしれないが、次のBufferを要求する
			if(store==null || store.getPutLength()==onBufferLength){
				logger.error("there is writeOrder but no buffer?cid:"+context.getPoolId()+ ":onBufferLength:"+onBufferLength);
			}
			if(store!=null){
				store.asyncBuffer(this, store);
			}
		}else{
			logger.debug("prepareWrite cid:"+ context.getPoolId() +":bufferSize:"+BuffersUtil.remaining(buffer));
		}
		return buffer;
	}
	
	//書き込み後、全部書いたbufferはリサイクルにまわす
	public void doneWrite(ByteBuffer[] prepareBuffers){
		logger.debug("doneWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		PoolManager.poolArrayInstance(prepareBuffers);//prepareで準備したBuffer配列は返却
		synchronized(workBuffer){
			Iterator<ByteBuffer> itr=workBuffer.iterator();
			while(itr.hasNext()){
				ByteBuffer buffer=itr.next();
				if( buffer.hasRemaining() ){
					break;
				}
				itr.remove();
				PoolManager.poolBufferInstance(buffer);
			}
			if(workBuffer.size()!=0){
				logger.debug("left workBuffer.size:"+workBuffer.size());
				//write可能になるのを待つ
				//context.queueuSelect();
				return;
			}
			prepareWriteReturnNull=false;
		}
		//次のBufferを要求する
		if(store!=null){
			store.asyncBuffer(this, store);
		}
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffer) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			return false;
		}
		logger.debug("onBuffer cid:"+ context.getPoolId()+ ":store:"+store+":buffer:"+BuffersUtil.remaining(buffer)+":size:"+workBuffer.size());
		boolean isQueueSelect=false;
		synchronized(workBuffer){
			onBufferLength+=BuffersUtil.remaining(buffer);
			if(workBuffer.size()==0&&buffer.length!=0){
				isQueueSelect=true;
			}
			for(int i=0;i<buffer.length;i++){
				workBuffer.add(buffer[i]);
			}
			//配列を返却
			PoolManager.poolArrayInstance(buffer);
			if(prepareWriteReturnNull){
				prepareWriteReturnNull=false;
				context.queueIO(ChannelContext.IO.WRITABLE);
			}
		}
		if(isQueueSelect){//write可能になるのを待つ
			context.queueuSelect();
		}
		return false;
	}

	public void onBufferEnd(Object userContext) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			logger.debug("onBufferEnd.store!=userContext cid:"+context.getPoolId()+":store:"+store);//こないと思う
			return;
		}
		logger.debug("onBufferEnd.cid:"+context.getPoolId());//こないと思う
		setStore(null);
	}
	
	public void onBufferFailure(Object userContext, Throwable falure) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			return;
		}
		logger.warn("onBufferFailure falure.cid:"+context.getPoolId(),falure);//こないと思う
		logger.warn("onBufferFailure now",new Exception());
		context.failure(falure);
		setStore(null);
	}

	/**
	 * asyncWriteに渡されたbufferの長さ
	 * @return
	 */
	public long getPutLength() {
		if(store!=null){
			return store.getPutLength();
		}
		return onBufferLength;
	}
}
