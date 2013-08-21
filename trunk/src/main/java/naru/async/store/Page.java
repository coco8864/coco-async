package naru.async.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import naru.async.cache.BufferCache;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.queuelet.QueueletContext;

import org.apache.log4j.Logger;

public class Page extends PoolBase{
	private static Logger logger=Logger.getLogger(Page.class);
	public static final long FREE_ID=-1;
	private static final int INIT_FREE_PAGE=16;
	public static final int PAGE_SIZE=36;
	
	private static StoreFile pageFile;
	private static PersistenceStore persistenceStore;
	private static BufferCache bufferCache=BufferCache.getInstance();
	
	public static void init(QueueletContext context,PersistenceStore persistenceStore,StoreFile pageFile){
		Page.persistenceStore=persistenceStore;
		Page.pageFile=pageFile;
		
		//�O��_�E�������ꍇ�Apage������Ă���,�K�v�ȏꍇ���J�o������
		persistenceStore.recoverPageFile(context,pageFile);
		
		for(int i=0;i<INIT_FREE_PAGE;i++){
			Page freePage=getSafeFreePage(persistenceStore.getTopFreePageId());
			if(freePage!=null){
//				logger.debug("$$$1 in:"+freePage.getPageId());
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
//			plainPage.unref(true);Buffer�͕ԋp���Ȃ��A...�Ȃ��Hterm��������ė��p���l���Ȃ����炩?
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
		int limit=128;//���̌Ăяo����freepage������E��
		synchronized(pageFile){
			Iterator<Page> itr=freePages.values().iterator();
			int count=freePages.size();
			while(itr.hasNext()){
				if(leftPageCount>=count || limit==0){
					break;
				}
				Page freePage=itr.next();
				itr.remove();
//				logger.debug("$$$2 out:"+freePage.pageId);
				freePage.nextPageId=persistenceStore.getTopFreePageId();
				freePage.save();
				persistenceStore.setTopFreePageId(freePage.getPageId());
				freePage.unref(true);
				count--;
				limit--;
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
	private boolean isLastBufferWrite;//�ŏI�o�b�t�@���������݉\���ۂ�

	public long setupCachePage(Page page){
		page.storeId=storeId;
		page.pageId=pageId;
		page.bufferLength=bufferLength;
		page.nextPageId=nextPageId;
		page.filePosition=filePosition;
		page.fileId=fileId;
		page.buffer=PoolManager.duplicateBuffers(buffer);
		//for debug
		//PoolManager.checkArrayInstance(buffer);
		return store.getPutLength();
	}
	
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
		isLastBufferWrite=false;
		super.recycle();
	}
	
	public void setStore(Store store){
//		logger.debug("setStore.store:"+store +":this.store:"+this.store);
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
//		logger.debug("loadPage.pageId:"+pageId);
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
		page.isLastBufferWrite=false;
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
			Iterator<Entry<Long,Page>> itr=freePages.entrySet().iterator();
			if(itr.hasNext()){
				Entry<Long,Page> entry=itr.next();
				itr.remove();
				freePage=entry.getValue();
//				logger.debug("$$$1 out:"+freePage.pageId);
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
		page.isLastBufferWrite=false;
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
	
	void recoverSavefreePage(long nextPageId){
		this.storeId=FREE_ID;
		this.nextPageId=nextPageId;
		save();
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
//		logger.debug("free."+this);
		//�t�@�C���Ɏ��Ԃ��Ȃ��ꍇ�A�����ʒu(pageId)���ė��p����K�v������B
		setStore(null);
		removePlainPage(this);//plainPage�ɗL��ꍇ�͍폜
		storeId=Store.FREE_ID;
		if(buffer!=null){
			logger.debug("free but remain buffer.");
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
		isLastBufferWrite=false;
		if(isPageFile){
			synchronized(pageFile){
				//������Page�����܂肷����
//				logger.debug("$$$2 in:"+pageId);
				Page prevPage=freePages.put(pageId,this);
				if(prevPage!=null){
					logger.error("duplicate free Page.pageId:"+pageId,new Exception());
					freePages.remove(pageId);
				}
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
		int len=this.buffer.length;
		if(len==0){
			return null;
		}
		return this.buffer[len-1];
	}
	private void setLastBuffer(ByteBuffer newLastBuffer){
		this.buffer[this.buffer.length-1]=newLastBuffer;
	}
	
	//����������Buffer��r������
	/*
	private void checkLastBuffer(){
		ByteBuffer lastBuffer=getLastBuffer();
		if(lastBuffer==null){//0����buffer�z�񂪓����Ă���
			return;
		}
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
	*/
	
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
//		logger.info("putBuffer this:"+System.identityHashCode(this)+":bufsid:"+System.identityHashCode(buffer));
		long length=BuffersUtil.remaining(buffer);
		logger.debug("putBuffer."+this +":" + bufferLength +":"+length);
		if(this.buffer==null||this.buffer.length==0){
			this.buffer=buffer;
			this.isLastBufferWrite=false;//�������buffer�͕ύX���Ă͂���
			//for debug
			//PoolManager.checkArrayInstance(buffer);
//			checkLastBuffer();������buffer�͍폜���悤�Ƃ������A���~
			this.bufferLength+=length;
			logger.debug("putBuffer org buffer null. result length"+this.bufferLength);
			return true;
		}
		ByteBuffer lastBuffer=getLastBuffer();
//		logger.warn("concat lastBuffer."+lastBuffer.capacity(),new Throwable());
		if(isLastBufferWrite && (lastBuffer.capacity()-lastBuffer.limit())>=length){
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
			//logger.debug("putBuffer concat buffer. result length:"+this.bufferLength);
			return true;
		}
		if(!isExpand){
			return false;
		}
		this.buffer=BuffersUtil.concatenate(this.buffer, buffer);
		this.isLastBufferWrite=false;
		/*
		ByteBuffer[] newBuffer=(ByteBuffer[])PoolManager.getArrayInstance(ByteBuffer.class, this.buffer.length+buffer.length);
		System.arraycopy(this.buffer, 0, newBuffer, 0, this.buffer.length);
		System.arraycopy(buffer, 0, newBuffer, this.buffer.length, buffer.length);
		PoolManager.poolArrayInstance(this.buffer);
		PoolManager.poolArrayInstance(buffer);
		this.buffer=newBuffer;
		*/
		//for debug
		//PoolManager.checkArrayInstance(buffer);
		//checkLastBuffer();
		this.bufferLength+=length;
		return true;
	}
	
	public synchronized boolean putBytes(byte[] bytes){
		return putBytes(bytes,0,bytes.length);
	}
	
	public synchronized boolean putBytes(byte[] bytes, int offset, int length){
		if(this.buffer==null||this.buffer.length==0){
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
			this.isLastBufferWrite=true;//�����ō����buffer�����珑������ł悢
//			logger.info("putBytes this:"+System.identityHashCode(this)+":bufsid:"+System.identityHashCode(buffer));
			//for debug
			//PoolManager.checkArrayInstance(buffer);
			
			this.bufferLength+=length;
			return true;
		}
		ByteBuffer lastBuffer=getLastBuffer();
//		if((lastBuffer.capacity()-lastBuffer.remaining())>=length){
		if(isLastBufferWrite && (lastBuffer.capacity()-lastBuffer.limit())>=length){
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
		/*
		ByteBuffer[] newBuffer=BuffersUtil.newByteBufferArray(this.buffer.length+1);
		System.arraycopy(this.buffer, 0, newBuffer, 0, this.buffer.length);
//		logger.info("putBytes this:"+System.identityHashCode(this)+":org bufsid:"+System.identityHashCode(buffer)+":new bufsid:"+System.identityHashCode(newBuffer));
		PoolManager.poolArrayInstance(this.buffer);
		this.buffer=newBuffer;
		*/
		//for debug
		//PoolManager.checkArrayInstance(buffer);
		
		long defaultBufferSize=PoolManager.getDefaultBufferSize();
		int allocBufferSize=(int)defaultBufferSize;
		ByteBuffer buf=null;
		if(length>defaultBufferSize){
			allocBufferSize=length;
		}
		buf=PoolManager.getBufferInstance((int)allocBufferSize);
		buf.put(bytes,offset,length);
		buf.flip();
		//checkLastBuffer();
		this.buffer=BuffersUtil.concatenate(this.buffer, buf,null);
		this.isLastBufferWrite=true;//�����ō����buffer�����珑������ł悢
		this.bufferLength+=length;
		return true;
	}
	
	public synchronized ByteBuffer[] getBuffer(){
		logger.debug("getBuffer."+this +":" + bufferLength);
		ByteBuffer[] buffer=this.buffer;
		this.bufferLength=0;
		
//		logger.info("getBuffer this:"+System.identityHashCode(this)+":bufsid:"+System.identityHashCode(buffer));
		this.buffer=null;
		this.isLastBufferWrite=false;
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
		/* ���ۂɓǂޑO��cache������ */
		ByteBuffer[] cacheBuffer=bufferCache.get(this);
		if(cacheBuffer!=null){
			this.buffer=cacheBuffer;
			this.isLastBufferWrite=false;//cache�͏��������Ă͂���
			return;
		}
		buffer=BuffersUtil.prepareBuffers(bufferLength);
		this.isLastBufferWrite=true;//�����ō����buffer�����珑������ł悢
//		logger.info("fillBuffer this:"+System.identityHashCode(this)+":bufsid:"+System.identityHashCode(buffer));
		//for debug
		//PoolManager.checkArrayInstance(buffer);
		
		bufferFile.read(buffer, filePosition);
		BuffersUtil.flipBuffers(buffer);
		
		/* ���ۂɓǂ񂾏ꍇ�ɂ�cache�ɓo�^ */
		if( store!=null && store.getKind()==Store.Kind.GET ){
			bufferCache.put(this,buffer);
		}
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
		
//		logger.info("flushBuffer this:"+System.identityHashCode(this)+":bufsid:"+System.identityHashCode(buffer));
		this.filePosition=bufferFile.write(buffer);
		this.buffer=null;
		this.isLastBufferWrite=false;
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
//		if(pageCache.put(this, false)){
//			return;
//		}
//		logger.debug("pageOut.storeId:"+storeId +":pageId:"+pageId+":digest:"+BuffersUtil.digestString(buffer));
//		logger.info("pageOut this:"+System.identityHashCode(this)+":bufsid:"+System.identityHashCode(buffer));
		StoreManager.asyncWritePage(this);
	}
	
	public void pageIn(){
		logger.debug("pageIn."+this);
		StoreManager.asyncReadPage(this);
	}
	
//	private PageCache pageCache;
	
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
