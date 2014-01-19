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
	private ChannelContext context;
	private ArrayList<ByteBuffer> workBuffer=new ArrayList<ByteBuffer>();
	private int workSize=8092;
	
	//setupで設定されrecycleされるまで保持する
	private Store store;
	private long onBufferLength;
	private long curBufferLength;
	
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
		curBufferLength=0;
		if(store!=null){
			store.unref();
			store=null;
		}
	}
	
	public void setup(){
		logger.debug("setup().cid:"+context.getPoolId()+":workBuffer:"+workBuffer);
		onBufferLength=0;
		store=Store.open(false);//storeはrecycleまで保持し続ける
		store.ref();//store処理が終わってもこのオブジェクトが生きている間保持する
		context.ref();//storeが生きている間contextを確保する
		store.asyncBuffer(this, store);
	}
	
	public void cleanup(){
		logger.debug("cleanup.cid:"+context.getPoolId());
		store.close(this,store);
	}
	
	//アプリから貰ったbufferは、putBuffersで詰め込む
	public void putBuffer(ByteBuffer[] buffer){
		logger.debug("putBuffer cid:"+ context.getPoolId()+":store:"+store +":len:"+BuffersUtil.remaining(buffer));
		store.putBuffer(buffer);
	}
	
	//queueIOする時にworkBufferがあることを確認しているので必ずworkBufferはある。
	public ByteBuffer[] prepareWrite(){
		logger.debug("prepareWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		ByteBuffer[] buffer=null;
		synchronized(this){
			buffer=BuffersUtil.toByteBufferArray(workBuffer);
		}
		//次のBufferを要求する
		store.asyncBuffer(this, store);
		return buffer;
	}
	
	//書き込み後、全部書いたbufferはリサイクルにまわす
	public void doneWrite(ByteBuffer[] prepareBuffers){
		logger.debug("doneWrite cid:"+ context.getPoolId() +":store:"+store+":size:"+workBuffer.size());
		PoolManager.poolArrayInstance(prepareBuffers);//prepareで準備したBuffer配列は返却
		synchronized(this){
			Iterator<ByteBuffer> itr=workBuffer.iterator();
			while(itr.hasNext()){
				ByteBuffer buffer=itr.next();
				if( buffer.hasRemaining() ){
					break;
				}
				itr.remove();
				PoolManager.poolBufferInstance(buffer);
			}
			curBufferLength=BuffersUtil.remaining(workBuffer);
		}
	}
	
	public boolean onBuffer(Object userContext, ByteBuffer[] buffer) {
		boolean next=false;
		long len=BuffersUtil.remaining(buffer);
		synchronized(this){
			onBufferLength+=len;
			curBufferLength+=len;
			for(int i=0;i<buffer.length;i++){
				workBuffer.add(buffer[i]);
			}
			//配列を返却
			PoolManager.poolArrayInstance(buffer);
			if(curBufferLength<workSize){
				next=true;
			}
		}
		//TODO writeがblockしていなければ,writeQに
		if(next){
			return true;
		}else{
			return false;
		}
	}

	public void onBufferEnd(Object userContext) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			logger.debug("onBufferEnd.store!=userContext cid:"+context.getPoolId()+":store:"+store);//こないと思う
			return;
		}
		synchronized(this){
			if(context==null){
				logger.error("duplicate WriterBuffer#onBufferEnd",new Throwable());
				return;
			}
			logger.debug("onBufferEnd.cid:"+context.getPoolId());//こないと思う
			context.unref();//storeが終了したのでcontextは開放してもよい
			context=null;
		}
	}
	
	public void onBufferFailure(Object userContext, Throwable falure) {
		if(store!=userContext){//callbackされる前にcloseされた場合
			return;
		}
		synchronized(this){
			if(context==null){
				logger.error("duplicate WriterBuffer#onBufferFailure",new Throwable());
				return;
			}
			logger.warn("onBufferFailure falure.cid:"+context.getPoolId(),falure);//こないと思う
			logger.warn("onBufferFailure now",new Exception());
			context.failure(falure);
			context.unref();//storeが終了したのでcontextは開放してもよい
			context=null;
		}
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
