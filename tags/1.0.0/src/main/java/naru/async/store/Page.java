package naru.async.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

import org.apache.log4j.Logger;

public class Page extends PoolBase{
	private static Logger logger=Logger.getLogger(Page.class);
	public static final long FREE_ID=-1;
	private static final int INIT_FREE_PAGE=16;
	public static final int PAGE_SIZE=36;
	
	private static StoreFile pageFile;
	private static PersistenceStore persistenceStore;
	
	public static void init(PersistenceStore persistenceStore,StoreFile pageFile){
		Page.persistenceStore=persistenceStore;
		Page.pageFile=pageFile;
		for(int i=0;i<INIT_FREE_PAGE;i++){
			Page freePage=getSafeFreePage(persistenceStore.getTopFreePageId());
			if(freePage!=null){
				freePages.put(freePage.getPageId(),freePage);
				persistenceStore.setTopFreePageId(freePage.getNextPageId());
			}else{
				break;
			}
		}
	}
	
	public static synchronized void term(){
		if(pageFile==null){
			return;
		}
		
		//FreePage�̃����N����������B
		saveFreePage(0);
		
		//Persicetence����Ă��Ȃ��y�[�W�́A������soreId���L�^���邱�ƂŖ��g�p�Ɩ���
		Iterator<Page> itr=plainPages.values().iterator();
		while(itr.hasNext()){//������java.util.ConcurrentModificationException����������
			Page plainPage=itr.next();
			itr.remove();
			plainPage.save();
//			plainPage.unref(true);Buffer�͕ԋp���Ȃ��A
		}
		
		pageFile.close();
		pageFile=null;
	}
	
	//���g�p��Page�Q
	private static Map<Long,Page> freePages=new HashMap<Long,Page>();
	static int getFreePageCount(){
		if(pageFile==null){
			return 0;
		}
		return freePages.size();
	}
	
	public static void saveFreePage(){
		saveFreePage(INIT_FREE_PAGE);
	}
	
	//TODO ����I��freePages���t�@�C����save���ă��������󂯂鏈���v
	public static void saveFreePage(int leftPageCount){
		if(pageFile==null){
			return;
		}
		logger.debug("saveFreePage freePage.size:"+freePages.size());
		synchronized(pageFile){
			Iterator<Page> itr=freePages.values().iterator();
			int count=freePages.size();
			while(itr.hasNext()){
				if(leftPageCount>=count){
					break;
				}
				Page freePage=itr.next();
				itr.remove();
				freePage.nextPageId=persistenceStore.getTopFreePageId();
				freePage.save();
				persistenceStore.setTopFreePageId(freePage.getPageId());
				freePage.unref(true);
				count--;
			}
		}
	}
	
	//���ۑ���Page�Q
	private static Map<Long,Page> plainPages=new HashMap<Long,Page>();
	static int getPlainPageCount(){
		return plainPages.size();
	}
	private static synchronized void addPlainPage(Page page){
//		synchronized(plainPages){
			plainPages.put(page.getPageId(), page);
//		}
	}
	private static synchronized void removePlainPage(Page page){
//		synchronized(plainPages){
			plainPages.remove(page.getPageId());
//		}
	}
	private static boolean isPlainPage(long pageId){
		return plainPages.containsKey(pageId);
	}
	private static boolean isFreePage(long pageId){
		return freePages.containsKey(pageId);
	}
	
	static Page nextCompressPage(long pageId){
		long nextPageId;
		Page nextPage=null;
		if(pageId==FREE_ID){
			nextPageId=-PAGE_SIZE;
		}else{
			nextPageId=pageId;
		}
		while(true){
			nextPageId+=PAGE_SIZE;
			if(nextPageId>=persistenceStore.getPageIdMax()){
				return null;
			}
			if(isFreePage(nextPageId)){//��������ɂ���FreePage
				continue;
			}
			if(isPlainPage(nextPageId)){//pageFile�ɂ͂Ȃ����A��������̎g�p�y�[�W
				continue;
			}
			nextPage=Page.loadPage(null,nextPageId);
			if(nextPage==null){
				return null;//�ȍ~���̂̂���Page�͂Ȃ�
			}
			if(nextPage.getStoreId()==Store.FREE_ID){//pageFile��ɂ�����FreePage
				nextPage.free(false);
				continue;
			}
			return nextPage;
		}
	}
	
	private static Page getSafeFreePage(long pageId){
		if(pageId<0){
			return null;
		}
		Page page=loadPage(null,pageId);
		if(page.storeId==FREE_ID){
			return page;
		}
		page.unref(true);
		return null;
	}
//	private boolean isInFile;//�t�@�C���Ɏ��Ԃ����邩�ۂ�
	private Store store;
	private long storeId;//�_��ID
	private long pageId;//�����ʒu
	private long bufferLength;
	private long nextPageId;
	private long filePosition;
	private int fileId;
	private ByteBuffer[] buffer;

	public void recycle() {
		setStore(null);
		storeId=0;
		bufferLength=0;
		nextPageId=FREE_ID;
		filePosition=0;
		fileId=-1;
		if(buffer!=null){
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
		super.recycle();
	}
	
	public void setStore(Store store){
		logger.debug("setStore.store:"+store +":this.store:"+this.store);
		if(store!=null){
			store.ref();
		}
		if(this.store!=null){
			this.store.unref();
		}
		this.store=store;
	}
	
	/**
	 * Page�t�@�C���ɕۑ�����Ă���Page��ǂݍ���
	 * Buffer�܂�load����킯�ł͂Ȃ�
	 * 
	 * @param store
	 * @param pageId
	 * @return
	 */
	public static Page loadPage(Store store,long pageId){
		logger.debug("loadPage.pageId:"+pageId);
		if(pageFile==null){
			throw new IllegalStateException("aleardy stoped StoreSystem.");
		}
		ByteBuffer pageBuffer=PoolManager.getBufferInstance(PAGE_SIZE);
		Page page=null;
		pageFile.read(pageBuffer, pageId);
		if(pageBuffer.position()<PAGE_SIZE){
//			logger.error("fail to loadPage.pageId:"+pageId);compress���͐���n�ŏo��
			PoolManager.poolBufferInstance(pageBuffer);
			return null;
		}
		pageBuffer.flip();
		
		page=(Page)PoolManager.getInstance(Page.class);
		page.setStore(store);
		page.pageId=pageId;
		//�i�[�\�������肵�Ă���
		page.filePosition=pageBuffer.getLong();//8
		page.nextPageId=pageBuffer.getLong();//8
		page.bufferLength=pageBuffer.getLong();//8
		page.storeId=pageBuffer.getLong();//8
		page.fileId=pageBuffer.getInt();//4
		page.buffer=null;
//		page.isInFile=true;
		PoolManager.poolBufferInstance(pageBuffer);
		if(store!=null && page.storeId!=store.getStoreId()){
			logger.error("storeId:"+store.getStoreId() + " is break. remove this");
			//store�����Ă���
			PoolManager.poolInstance(page);
			return null;
//			throw new RuntimeException("fail to loadPage.store is break.storeId:"+store.getStoreId());
		}
		return page;
	}
	
	/**
	 * PUTGET or PUT��Page:�ŏ��́APage�t�@�C���ɕۑ�����Ȃ�
	 * 
	 * @param store Store
	 * @param prev �Spage
	 * @return
	 */
	public static Page allocPage(Store store,Page prev){
		if(pageFile==null){
			throw new IllegalStateException("aleardy stoped StoreSystem.");
		}
		Page page;
		synchronized(pageFile){
			Page freePage=null;
			long topFreePageId=persistenceStore.getTopFreePageId();
			if(freePages.size()>0){
				freePage=freePages.remove(0);
			}else if(topFreePageId!=FREE_ID){
				freePage=getSafeFreePage(topFreePageId);
				if(freePage!=null){
					topFreePageId=freePage.getNextPageId();
				}else{
					topFreePageId=FREE_ID;
				}
				persistenceStore.setTopFreePageId(topFreePageId);
			}
			if(freePage==null){
				page=(Page)PoolManager.getInstance(Page.class);
				page.pageId=persistenceStore.nextPageId();
//				page.isInFile=false;
			}else{
				page=freePage;
			}
		}
		page.storeId=store.getStoreId();
		page.nextPageId=FREE_ID;
		page.bufferLength=0;
		page.buffer=null;
		page.setStore(store);
		if(prev!=null){
			prev.nextPageId=page.pageId;
		}
		addPlainPage(page);
		logger.debug("allocPage.page:"+page);
		return page;
	}
	
	public String toString(){
		return "pageId:"+pageId +":"+super.toString();
	}
	
	public void save(){
		logger.debug("save."+this);
		ByteBuffer pageBuffer=PoolManager.getBufferInstance(PAGE_SIZE);
		//�i�[�\�������肵�Ă���
		pageBuffer.putLong(filePosition);//8
		pageBuffer.putLong(nextPageId);//8
		pageBuffer.putLong(bufferLength);//8
		pageBuffer.putLong(storeId);//8
		pageBuffer.putInt(fileId);//4
		pageBuffer.flip();
		pageFile.write(pageBuffer, pageId);
//		isInFile=true;
	}

	public void free(boolean isPageFile){
		free(isPageFile,false);
	}
	
	/**
	 * 
	 * @param isPageFile�@pageFile������g�p�ɂ��邩�ۂ�
	 * @param isPageFree�@compress�̉����ŌĂяo���ꍇ�́AfreePage�����܂�߂��Ȃ��悤�ɂ���
	 * 
	 */
	public synchronized void free(boolean isPageFile,boolean isSaveFree){
		logger.debug("free."+this);
		//�t�@�C���Ɏ��Ԃ��Ȃ��ꍇ�A�����ʒu(pageId)���ė��p����K�v������B
		setStore(null);
		removePlainPage(this);//plainPage�ɗL��ꍇ�͍폜
		storeId=Store.FREE_ID;
		if(buffer!=null){
			logger.debug("free but remain buffer.");
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
		if(isPageFile){
			synchronized(pageFile){	
				freePages.put(pageId,this);
			}
			//TODO
			if(isSaveFree){
				saveFreePage();			
			}
		}else{
			unref();
		}
	}
	
	private ByteBuffer getLastBuffer(){
		return this.buffer[this.buffer.length-1];
	}
	private void setLastBuffer(ByteBuffer newLastBuffer){
		this.buffer[this.buffer.length-1]=newLastBuffer;
	}
	
	//����������Buffer��r������
	private void checkLastBuffer(){
		ByteBuffer lastBuffer=getLastBuffer();
		int defaultBufferSize=PoolManager.getDefaultBufferSize();
		if(lastBuffer.capacity()>=defaultBufferSize){
			//lastBuffer.compact();compact��array�����������
			//lastBuffer.flip();
			return;
		}
		ByteBuffer buf=PoolManager.getBufferInstance();
		buf.put(lastBuffer);
		buf.flip();
		PoolManager.poolBufferInstance(lastBuffer);
		setLastBuffer(buf);
//		this.buffer[this.buffer.length-1]=buf;
	}
	
	public synchronized boolean putBuffer(ByteBuffer[] buffer){
		return putBuffer(buffer,false);
	}
	
	public synchronized boolean putBuffer(ByteBuffer buffer,boolean isExpand){
		ByteBuffer[] buffers=BuffersUtil.toByteBufferArray(buffer);
		boolean result=putBuffer(buffers,isExpand);
		if(result==false){
			PoolManager.poolArrayInstance(buffers);
		}
		return result;
	}
	
	public synchronized boolean putBuffer(ByteBuffer[] buffer,boolean isExpand){
		long length=BuffersUtil.remaining(buffer);
		logger.debug("putBuffer."+this +":" + bufferLength +":"+length);
		if(this.buffer==null){
			this.buffer=buffer;
			checkLastBuffer();
			this.bufferLength+=length;
			logger.debug("putBuffer org buffer null. result length"+this.bufferLength);
			return true;
		}
		ByteBuffer lastBuffer=getLastBuffer();
//		if((lastBuffer.capacity()-lastBuffer.remaining())>=length){
		if((lastBuffer.capacity()-lastBuffer.limit())>=length){
//			lastBuffer.compact();array��j�󂷂�̂Ŏg���Ȃ�
			int orgPosition=lastBuffer.position();
			lastBuffer.position(lastBuffer.limit());
			lastBuffer.limit(lastBuffer.capacity());
			for(int i=0;i<buffer.length;i++){
				lastBuffer.put(buffer[i]);
				PoolManager.poolBufferInstance(buffer[i]);
			}
			PoolManager.poolArrayInstance(buffer);
			lastBuffer.flip();
			lastBuffer.position(orgPosition);
			this.bufferLength+=length;
			logger.debug("putBuffer concat buffer. result length:"+this.bufferLength);
			return true;
		}
		if(!isExpand){
			return false;
		}
		ByteBuffer[] newBuffer=(ByteBuffer[])PoolManager.getArrayInstance(ByteBuffer.class, this.buffer.length+buffer.length);
		System.arraycopy(this.buffer, 0, newBuffer, 0, this.buffer.length);
		System.arraycopy(buffer, 0, newBuffer, this.buffer.length, buffer.length);
		PoolManager.poolArrayInstance(this.buffer);
		PoolManager.poolArrayInstance(buffer);
		this.buffer=newBuffer;
		checkLastBuffer();
		this.bufferLength+=length;
		return true;
	}
	
	public synchronized boolean putBytes(byte[] bytes){
		return putBytes(bytes,0,bytes.length);
	}
	
	public synchronized boolean putBytes(byte[] bytes, int offset, int length){
		if(this.buffer==null){
			int defaultBufferSize=PoolManager.getDefaultBufferSize();
			int allocBufferSize=defaultBufferSize;
			if(length>defaultBufferSize){
				allocBufferSize=length;
			}
			ByteBuffer buf=PoolManager.getBufferInstance(allocBufferSize);
			try {
				buf.put(bytes,offset,length);
			} catch (RuntimeException e) {
				logger.error("buf.put error.offset:"+offset + ":length:"+length+":allocBufferSize:"+allocBufferSize,e);
				return false;
			}
			buf.flip();
			this.buffer=BuffersUtil.toByteBufferArray(buf);
			this.bufferLength+=length;
			return true;
		}
		ByteBuffer lastBuffer=getLastBuffer();
//		if((lastBuffer.capacity()-lastBuffer.remaining())>=length){
		if((lastBuffer.capacity()-lastBuffer.limit())>=length){
//			lastBuffer.compact();array��j�󂷂�̂Ŏg���Ȃ�
			int orgPosition=lastBuffer.position();
			lastBuffer.position(lastBuffer.limit());
			lastBuffer.limit(lastBuffer.capacity());
			lastBuffer.put(bytes,offset,length);
			lastBuffer.flip();
			lastBuffer.position(orgPosition);
			this.bufferLength+=length;
			return true;
		}
		ByteBuffer[] newBuffer=BuffersUtil.newByteBufferArray(this.buffer.length+1);
		System.arraycopy(this.buffer, 0, newBuffer, 0, this.buffer.length);
		PoolManager.poolArrayInstance(this.buffer);
		this.buffer=newBuffer;
		
		long defaultBufferSize=PoolManager.getDefaultBufferSize();
		int allocBufferSize=(int)defaultBufferSize;
		ByteBuffer buf=null;
		if(length>defaultBufferSize){
			allocBufferSize=length;
		}
		buf=PoolManager.getBufferInstance((int)allocBufferSize);
		
		buf.put(bytes,offset,length);
		buf.flip();
		newBuffer[newBuffer.length-1]=buf;//�Ō�ɒǉ�����
		checkLastBuffer();
		this.bufferLength+=length;
		return true;
	}
	
	public synchronized ByteBuffer[] getBuffer(){
		logger.debug("getBuffer."+this +":" + bufferLength);
		ByteBuffer[] buffer=this.buffer;
		this.bufferLength=0;
		this.buffer=null;
		return buffer;
	}
	
	/**
	 * Buffer�t�@�C������buffer�ɓǂݍ��ރ��\�b�h
	 * 
	 * @param bufferFile�@fildId�ɑΉ�����StoreFile
	 * @throws IOException
	 */
	public synchronized void fillBuffer(StoreFile bufferFile) throws IOException{
		logger.debug("fillBuffer."+this);
		buffer=BuffersUtil.prepareBuffers(bufferLength);
		bufferFile.read(buffer, filePosition);
		BuffersUtil.flipBuffers(buffer);
	}
	
	/**
	 * buffer��buffer�t�@�C���ɋL�����郁�\�b�h
	 * @param fileId
	 * @param bufferFile
	 * @throws IOException
	 */
	public void flushBuffer(int fileId,StoreFile bufferFile) throws IOException{
		logger.debug("flushBuffer."+this);
		if( logger.isDebugEnabled() ){
			long l=BuffersUtil.remaining(buffer);
			if(l!=bufferLength){
				logger.error("length error.pageId:"+pageId +"l:"+l+":bufferLength:"+bufferLength,new Exception());
			}
		}
		this.fileId=fileId;
		this.filePosition=bufferFile.write(buffer);
		this.buffer=null;
	}
	
	public long getPageId() {
		return pageId;
	}

	public long getBufferLength() {
		return bufferLength;
	}

	public long getNextPageId() {
		return nextPageId;
	}
	
	public int getFileId() {
		return fileId;
	}

	public long getFilePosition() {
		return filePosition;
	}

	public long getStoreId() {
		return storeId;
	}

	public void pageOut(){
//		logger.debug("pageOut.storeId:"+storeId +":pageId:"+pageId+":digest:"+BuffersUtil.digestString(buffer));
		StoreManager.asyncWritePage(this);
	}
	
	public void pageIn(){
		logger.debug("pageIn."+this);
		StoreManager.asyncReadPage(this);
	}
	
	public void onPageOut(){
		logger.debug("onPageOut.this:"+this);
		save();
		removePlainPage(this);
		if(store!=null){
			store.onPageOut(this);
		}
	}
	
	public void onPageIn(){
//		logger.debug("onPageIn.storeId:"+storeId +":pageId:"+pageId+":digest:"+BuffersUtil.digestString(buffer));
		if(store!=null){
			store.onPageIn(this);
		}else{
			logger.error("onPageIn.store=null",new Exception());
		}
	}
	
	public void onFailure(Throwable failure){
		if(store!=null){
			store.onFailure(failure);
		}else{
			logger.error("onFailure.store=null",new Exception());
		}
	}

}