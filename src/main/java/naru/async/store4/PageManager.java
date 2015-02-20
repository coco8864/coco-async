package naru.async.store4;

import java.lang.ref.ReferenceQueue;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import naru.async.pool.LocalPoolManager;
import naru.async.pool.Pool;

public class PageManager {
	private static PageManager instance=new PageManager();
	private static final ByteBuffer ZERO_BUFFER=ByteBuffer.allocate(0);
	private static final int BUFFER_SIZE_UNIT=1024;//�o�b�t�@�[�T�C�Y�̒P��
	
	//freeSize,useSize,swapOutSize | totalMaxSize
	//fileSize,
	
	public static PageManager getInstance(){
		return instance;
	}
	//Page�ɏ�������ByteBuffer��GC���ꂽ�ۂɓo�^�����refernceQueue
	private ReferenceQueue pageBufferReferenceQueue = new ReferenceQueue();
	
	//memory�����Ȃ��Ȃ��swapOut�����Page�Q
	private LinkedBlockingDeque<Page> pageOutQueue=new LinkedBlockingDeque<Page>();
	//memory�ɗ]�T���łĂ����swapIn�����Page�Q
	private LinkedBlockingDeque<Page> pageInQueue=new LinkedBlockingDeque<Page>();
	
	//�������ɖ��g�p��ByteBuffer������Page�@WRITE mode��pool����Ă���
	private Map<Integer,LinkedBlockingDeque<Page>> byteBufferPool=Collections.synchronizedMap(new HashMap<Integer,LinkedBlockingDeque<Page>>());
	void poolBufferFreePage(Page page){
		int length=page.getBytes().length;
		LinkedBlockingDeque<Page> pages=byteBufferPool.get(length);
		if(pages!=null){
			pages.addFirst(page);
		}
		throw new RuntimeException("poolBufferFreePage length error."+length);
	}
	Page getBufferFreePage(int length){
		LinkedBlockingDeque<Page> pages=byteBufferPool.get(length);
		if(pages!=null){
			return pages.pollFirst();
		}
		return null;
	}
	
	//pagePool���g�pPage�@bytes�͐ݒ肳��Ă��Ȃ�
	private LinkedBlockingDeque<Page> pagePool=new LinkedBlockingDeque<Page>();
	void poolFreePage(Page page){
		pagePool.addFirst(page);
	}
	Page getFreePage(){
		return pagePool.pollFirst();
	}
	
	//�g�p�A���g�p�Amode�֌W�Ȃ��Abyte[]����Page��������
	private Map<byte[],Page> bytesPages;//byteBuffer->Page�p
	void changeBytesPage(byte[] oldBytes,byte[] newBytes,Page page){
		synchronized(bytesPages){
			if(oldBytes!=null){
				bytesPages.remove(oldBytes);
			}
			if(newBytes!=null){
				bytesPages.put(newBytes, page);
			}
		}
	}
	Page getPageByByes(byte[] bytes){
		return bytesPages.get(bytes);
	}
	//�g�p�A���g�p�Amode�֌W�Ȃ��ApageId����Page��������
	private Map<Integer,Page> idPages;//byteBuffer->Page�p
	void putIdPage(Page page){
		synchronized(idPages){
			idPages.put(page.getPageId(), page);
		}
	}
	Page getIdPage(Integer pageId){
		return idPages.get(pageId);
	}
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
	public ByteBuffer getBuffer() {
		return getBuffer(1024);
	}
	
	public ByteBuffer getBuffer(int bufferSize) {
		if(bufferSize==0){
			return ZERO_BUFFER;
		}
		ByteBuffer buffer=null;
		int actualBufferSize=actualBufferSize(bufferSize);
		BlockingDeque<Page> pages=byteBufferPool.get(actualBufferSize);
		if(pages!=null){
			Page page=pages.pollFirst();
			if(page!=null){
				buffer=page.getBuffer();
				buffer.limit(bufferSize);
				return buffer;
			}
		}
		
		
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
