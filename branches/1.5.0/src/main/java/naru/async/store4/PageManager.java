package naru.async.store4;

import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import naru.async.pool.LocalPoolManager;
import naru.async.pool.Pool;

public class PageManager {
	private static PageManager instance=new PageManager();
	private static final ByteBuffer ZERO_BUFFER=ByteBuffer.allocate(0);
	private static final int BUFFER_SIZE_UNIT=1024;//バッファーサイズの単位
	
	public static PageManager getInstance(){
		return instance;
	}
	//Pageに所属するByteBufferがGCされた際に登録されるrefernceQueue
    private ReferenceQueue pageBufferReferenceQueue = new ReferenceQueue();
	
	private Map<Integer,Store> permStores=new HashMap<Integer,Store>();//不揮発Store parmStore.sarファイルに保存
	private Map<Integer,Page> permPages=new HashMap<Integer,Page>();//不揮発Page parmPage.sarファイルに保存

	private List<Page> tempFirstCache=new LinkedList<Page>();//すぐ使われる可能性が高い
	private List<Page> tempSecondCache=new LinkedList<Page>();//すぐ使われる可能性が低い
	private List<Page> tempOffMemCache=new LinkedList<Page>();//swapoutされている
	
	private Map<byte[],Page> arrayPages;//byteBuffer->Page用
	private Map<Integer,Map<Long,Page>> filePositionPages;
	
	//pagePool未使用Page　arrayは設定されていない
	private List<Page> pagePool=new LinkedList<Page>();
	//byteBufferPool 未使用ByteBufferのpool ByteBufferは、Pageを持っている
	private Map<Integer,List<ByteBuffer>> byteBufferPool=new HashMap<Integer,List<ByteBuffer>>();
	//byteArrayPool 未使用byte[]のpool
	private Map<Integer,List<byte[]>> byteArrayPool=new HashMap<Integer,List<byte[]>>();
	
	private int actualBufferSize(int bufferSize){
		return (((bufferSize-1)/(BUFFER_SIZE_UNIT))+1)*BUFFER_SIZE_UNIT;//1024の倍数に調整する
	}
	
	ByteBuffer allocateMemoryPage(int size){
		synchronized(byteBufferPool){
			List<ByteBuffer>pool=byteBufferPool.get(size);
			if(pool!=null&&pool.size()>0){
				ByteBuffer byteBuffer=pool.remove(0);
				return byteBuffer;
			}
		}
		Page page;
		synchronized(pagePool){
			if(pagePool.size()==0){
				xxx
			}
			page=pagePool.remove(0);
		}
		
		
		
	}
	
	Page allocateFilePage(int fileId,long offset,int length){
		
	}
	
	/**
	 * 以降buffer関連のメソッド,BufferPoolにデリケート
	 * @return
	 */
	public ByteBuffer getBufferInstance() {
		return getBufferInstance(1024);
	}
	
	public ByteBuffer getBufferInstance(int bufferSize) {
		if(bufferSize==0){
			return ZERO_BUFFER;
		}
		int actualBufferSize=actualBufferSize(bufferSize);
		Pool pool=null;
		pool=byteBufferPoolMap.get(actualBufferSize);
		if(pool==null){
			pool=addBufferPool(actualBufferSize);
		}
		ByteBuffer buffer=(ByteBuffer)pool.getInstance();
		buffer.limit(bufferSize);
		return buffer;
	}
	
	public long pushPage(ByteBuffer[] buffer,boolean isFast){
		return 0;
	}
	
	public ByteBuffer[] popPage(long id,boolean callback){
		return null;
	}
	
	public void removePage(int pageId){
	}
	
	ReferenceQueue getReferenceQueue(){
		return pageBufferReferenceQueue;
	}

	private int byteBufferLifeCounter=0;
	synchronized int byteBufferLifeCounter(){
		byteBufferLifeCounter++;
		return byteBufferLifeCounter;
	}
	
}
