package naru.async.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;

/**
 * bufferéÊìæÇ™äÆóπÇµÇΩèÍçáÅAthisÇ…notifyÇ∑ÇÈ
 * @author Naru
 *
 */
public class StoreBufferGetter extends PoolBase implements BufferGetter {
	private static Logger logger=Logger.getLogger(StoreBufferGetter.class);
	
	private boolean isEnd=false;
	private ArrayList<ByteBuffer[]>buffersList=new ArrayList<ByteBuffer[]>();

	public void recycle() {
		isEnd=false;
		buffersList.clear();
		super.recycle();
	}
	
	public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
		buffersList.add(buffers);
		return true;
	}

	public void onBufferEnd(Object userContext) {
		synchronized(this){
			isEnd=true;
			notify();
		}
	}

	public void onBufferFailure(Throwable falure, Object userContext) {
		logger.error("StoreBufferGetter error.store:"+userContext,falure);
		synchronized(this){
			isEnd=true;
			notify();
		}
	}
	
	public boolean isEnd(){
		return isEnd;
	}
	
	public ByteBuffer[] getBuffer(){
		int size=buffersList.size();
		if(size==0){
			return null;
		}else if(size==1){
			return buffersList.get(0);
		}
		int bufferCount=0;
		for(int i=0;i<size;i++){
			bufferCount+=buffersList.get(i).length;
		}
		ByteBuffer[] buffers=BuffersUtil.newByteBufferArray(bufferCount);
		bufferCount=0;
		for(int i=0;i<size;i++){
			ByteBuffer[] tmp=buffersList.get(i);
			for(int j=0;j<tmp.length;j++,bufferCount++){
				buffers[bufferCount]=tmp[j];
			}
		}
		return buffers;
	}
}
