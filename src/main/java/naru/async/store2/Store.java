package naru.async.store2;

import java.nio.ByteBuffer;
import java.util.LinkedList;

import naru.async.BufferGetter;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

public class Store extends PoolBase {
	class PageInfo{
		private int id;
		private int length;
	}
	private long sid;
	private String digest;
	private LinkedList<PageInfo> pages=new LinkedList<PageInfo>();
	private LinkedList<ByteBuffer> currentBuffers=new LinkedList<ByteBuffer>();
	private int currentLength;
	
	private BufferGetter bufferGetter;
	private Object userContext;
	private boolean canCallback;//callbackしてよいか否か
	private long borderLength;
	
	@Override
	public void recycle() {
		borderLength=PoolManager.getDefaultBufferSize()/2;
		this.canCallback=false;
	}
	
	public void asyncBuffer(BufferGetter bufferGetter,Object userContext){
		this.bufferGetter=bufferGetter;
		this.userContext=userContext;
		this.canCallback=true;
	}
	
	/* この処理でIOが走行してはいけない
	 * この処理からcallbackされる可能性はある
	 */
	public void putBuffer(ByteBuffer[] buffers){
		if(borderLength<=0||currentLength<borderLength){
			for(ByteBuffer buffer:buffers){
				currentLength+=buffer.remaining();
				currentBuffers.add(buffer);
			}
		}
		//currentBufferrsとbuffersを１bufferにまとめて
		//PagePool.pushPage(buffer);
		//
	}
	
}
