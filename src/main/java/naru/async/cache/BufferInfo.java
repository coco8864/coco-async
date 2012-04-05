package naru.async.cache;

import java.nio.ByteBuffer;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.async.store.StoreManager;

/* bufferをpoolで眠らせるより,何がし有効なデータを保持しているbufferは積極的に再利用するポリシー */
//default　Buffer poolの状況をみてcacheからの追い出しを行う
public class BufferInfo extends PoolBase{
	public static BufferInfo create(ByteBuffer[] buffer,long totalLength,FileInfo fileInfo){
		BufferInfo bufferInfo=(BufferInfo)PoolManager.getInstance(BufferInfo.class);
		bufferInfo.init(buffer, totalLength,-1,fileInfo);
		return bufferInfo;
	}
	public static BufferInfo create(ByteBuffer[] buffer,long totalLength,long storeId){
		BufferInfo bufferInfo=(BufferInfo)PoolManager.getInstance(BufferInfo.class);
		bufferInfo.init(buffer, totalLength,storeId,null);
		return bufferInfo;
	}
	
	@Override
	public void recycle() {
		if(buffer!=null){
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
		if(fileInfo!=null){
			fileInfo.unref();
		}
	}
	
	private void init(ByteBuffer[] buffer,long totalLength,long storeId,FileInfo fileInfo){
		this.buffer=PoolManager.duplicateBuffers(buffer);
		long length=BuffersUtil.remaining(buffer);
//		this.totalLength=totalLength;
		this.lengthRate=(float)length/(float)totalLength;
		this.cacheTime=System.currentTimeMillis();
		this.storeId=storeId;
		if(fileInfo!=null){
			fileInfo.ref(false);
		}
		this.fileInfo=fileInfo;
	}
	private long storeId;
	private FileInfo fileInfo;
	
	private ByteBuffer[] buffer;
	/* 統計情報 */
//	private long length;//当該バッファの長さ
//	private long totalLength;//データ全体の長さ,
	
	private float lengthRate;
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
	
	private float lastScore=0.0f;
	
	/* 当該infoの不要度(大きいと捨てられる可能性が高い) */
	public float getScore(long now){
		float lastScore=0.0f;
		long orgIntervalCount=intervalCount;
		intervalCount=0;
		if(orgIntervalCount>=1){
			return lastScore;
		}
		if(totalCount==0){
			totalCount=1;
		}
		//TODO 精査する事
		lastScore=(float)(now-(long)lastTime)/((float)totalCount*lengthRate);
		return lastScore;
	}
	
	public float getLastScore(){
		return lastScore;
	}
	public boolean isChange() {
		if(fileInfo!=null){
			return fileInfo.isChange();
		}
		long totalLength=StoreManager.getStoreLength(storeId);
		return(totalLength<0);
	}
}
