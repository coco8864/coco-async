package naru.async.store2;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import naru.async.Log;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

import org.apache.log4j.Logger;

public class Page extends PoolBase {
	private static Logger logger=Logger.getLogger(Page.class);
	private static PagePool pagePool=PagePool.getInstance();
	private Store store;
	private long storeId;//ò_óùID
	private long pageId;//ï®óùà íu

	private Page prevPage;
	private Page nextPage;
	private long nextPageId;

	private long pageLength;
	
	private int fileId;
	private long filePosition;
	
	private List<ByteBuffer> buffers=new ArrayList<ByteBuffer>();

	private long borderLength;
	
	@Override
	public void recycle() {
		borderLength=PoolManager.getDefaultBufferSize()/2;
	}
	
	public synchronized boolean putBuffer(ByteBuffer[] buffers){
		if(borderLength>0&&pageLength>=borderLength){
			return false;
		}
		for(ByteBuffer buffer:buffers){
			pageLength+=buffer.remaining();
			this.buffers.add(buffer);
		}
		return true;
	}
	
	public synchronized ByteBuffer[] getBuffer(){
		Log.debug(logger,"getBuffer.",this,":",pageLength);
		ByteBuffer[] buffer=BuffersUtil.toByteBufferArray(buffers);
		this.pageLength=0;
		this.buffers.clear();
		return buffer;
	}

}
