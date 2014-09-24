package naru.async.store2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import naru.async.Log;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;

import org.apache.log4j.Logger;

public class Page extends PoolBase {
	private static Logger logger=Logger.getLogger(Page.class);
	private Store store;
	private long storeId;//ò_óùID
	private long pageId;//ï®óùà íu
	private long nextPageId;
	private Page nextPage;
	private long filePosition;
	private int fileId;
	private long bufferLength;
	private List<ByteBuffer> buffers=new ArrayList<ByteBuffer>();
	@Override
	public void recycle() {
	}
	
	public synchronized boolean putBuffer(ByteBuffer[] buffers){
		for(ByteBuffer buffer:buffers){
			bufferLength+=buffer.remaining();
			this.buffers.add(buffer);
		}
		return true;
	}
	
	public synchronized ByteBuffer[] getBuffer(){
		Log.debug(logger,"getBuffer.",this,":",bufferLength);
		ByteBuffer[] buffer=BuffersUtil.toByteBufferArray(buffers);
		this.bufferLength=0;
		this.buffers.clear();
		return buffer;
	}

}
