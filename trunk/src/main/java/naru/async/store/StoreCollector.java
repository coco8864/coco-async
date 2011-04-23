package naru.async.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public class StoreCollector extends PoolBase implements BufferGetter {
	private static Logger logger=Logger.getLogger(StoreCollector.class);
	
	private boolean processing;
	private List<ByteBuffer> collectBuffers=new ArrayList<ByteBuffer>();
	
	@Override
	public void recycle() {
		collectBuffers.clear();
	}
	
	public synchronized ByteBuffer[] collect(Store store){
		processing=true;
		while(processing){
			try {
				store.asyncBuffer(this, null);
				wait();
			} catch (InterruptedException e) {
			}
		}
		int count=collectBuffers.size();
		if(count==0){
			return null;
		}
		ByteBuffer[] buffers=collectBuffers.toArray(BuffersUtil.newByteBufferArray(count));
		collectBuffers.clear();
		return buffers;
	}

	public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
		for(ByteBuffer buffer:buffers){
			collectBuffers.add(buffer);
		}
		PoolManager.poolArrayInstance(buffers);//配列オブジェクトだけ返却
		return true;
	}

	public void onBufferEnd(Object userContext) {
		synchronized(this){
			processing=false;
			notify();
		}
	}

	public void onBufferFailure(Object userContext, Throwable failure) {
		logger.error("StoreCollector store read error.",failure);
		Iterator<ByteBuffer> itr=collectBuffers.iterator();
		while(itr.hasNext()){
			PoolManager.poolBufferInstance(itr.next());
			itr.remove();
		}
		onBufferEnd(userContext);
	}

}
