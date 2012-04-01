package naru.async.cache;

import java.nio.ByteBuffer;

import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

/* bufferをpoolで眠らせるより,何がし有効なデータを保持しているbufferは積極的に再利用するポリシー */
//default　Buffer poolの状況をみてcacheからの追い出しを行う
public class BufferInfo extends PoolBase{
	public static BufferInfo create(ByteBuffer[] buffer,long totalLength){
		BufferInfo bufferInfo=(BufferInfo)PoolManager.getInstance(BufferInfo.class);
		bufferInfo.init(buffer, totalLength);
		return bufferInfo;
	}
	
	@Override
	public void recycle() {
		if(buffer!=null){
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
	}
	private void init(ByteBuffer[] buffer,long totalLength){
		this.buffer=PoolManager.duplicateBuffers(buffer);
		this.totalLength=totalLength;
		this.cacheTime=System.currentTimeMillis();
	}
	private ByteBuffer[] buffer;
	/* 統計情報 */
	private long totalLength;//全体の長さ,
	private long cacheTime;
	private long lastTime;
	private int totalCount;
	private int intervalCount;//これが1以上の場合追い出さない
	
	public ByteBuffer[] duplicateBuffers(){
		return PoolManager.duplicateBuffers(buffer);
	}
	
	public void ref(){
		totalCount++;
		intervalCount++;
		lastTime=System.currentTimeMillis();
		super.ref();
	}
	
	public int intervalReset(){
		int orgIntervalCount=intervalCount;
		intervalCount=0;
		return orgIntervalCount;
	}
}
