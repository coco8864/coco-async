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
	private static final int BUFFER_SIZE_UNIT=1024;//�o�b�t�@�[�T�C�Y�̒P��
	
	public static PageManager getInstance(){
		return instance;
	}
	//Page�ɏ�������ByteBuffer��GC���ꂽ�ۂɓo�^�����refernceQueue
    private ReferenceQueue pageBufferReferenceQueue = new ReferenceQueue();
	
	private Map<Integer,Store> permStores=new HashMap<Integer,Store>();//�s����Store parmStore.sar�t�@�C���ɕۑ�
	private Map<Integer,Page> permPages=new HashMap<Integer,Page>();//�s����Page parmPage.sar�t�@�C���ɕۑ�

	private List<Page> tempFirstCache=new LinkedList<Page>();//�����g����\��������
	private List<Page> tempSecondCache=new LinkedList<Page>();//�����g����\�����Ⴂ
	private List<Page> tempOffMemCache=new LinkedList<Page>();//swapout����Ă���
	
	private Map<byte[],Page> arrayPages;//byteBuffer->Page�p
	private Map<Integer,Map<Long,Page>> filePositionPages;
	
	//pagePool���g�pPage�@array�͐ݒ肳��Ă��Ȃ�
	private List<Page> pagePool=new LinkedList<Page>();
	//byteBufferPool ���g�pByteBuffer��pool ByteBuffer�́APage�������Ă���
	private Map<Integer,List<ByteBuffer>> byteBufferPool=new HashMap<Integer,List<ByteBuffer>>();
	//byteArrayPool ���g�pbyte[]��pool
	private Map<Integer,List<byte[]>> byteArrayPool=new HashMap<Integer,List<byte[]>>();
	
	private int actualBufferSize(int bufferSize){
		return (((bufferSize-1)/(BUFFER_SIZE_UNIT))+1)*BUFFER_SIZE_UNIT;//1024�̔{���ɒ�������
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
	 * �ȍ~buffer�֘A�̃��\�b�h,BufferPool�Ƀf���P�[�g
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
