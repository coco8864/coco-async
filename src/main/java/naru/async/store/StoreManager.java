package naru.async.store;

//TODO offline compress
//TODO freePage�̃v�[�����Aonlinecompress�̉����ŕۑ�

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.Timer;
import naru.async.timer.TimerManager;
import naru.queuelet.Queuelet;
import naru.queuelet.QueueletContext;

public class StoreManager {
	private static final int HASH_LOCK_INDEX=67;
	private static final int MAX_FILE_ID=16;
	private static final String DUMP_FILE_NAME="storeDump.zip";
	
	private static Logger logger=Logger.getLogger(StoreManager.class);
	private static StoreFile bufferFiles[];
	private static ArrayBlockingQueue<Integer> writeFileIds;
	private static QueueletContext readContext;
	private static QueueletContext writeContext;
	private static QueueletContext dispatchContext;
	private static PersistenceStore persistenceStore;
	private static long compressInterval=60000;
	//dumpFile:persistenceStoreFile�Ɠ����f�B���N�g����DUMP_FILE_NAME�ō쐬
	private static File dumpFile=null;
	
	private static StoreStastics storeStastics;
	
	
	public static Queuelet getBufferFileWriter(){
		return new BufferFileWriter();
	}
	public static Queuelet getBufferFileReader(){
		return new BufferFileReader();
	}
	public static Queuelet getStoreDispatcher(){
		return new Dispatcher();
	}
	
	public static void asyncReadPage(Page page){
		readContext.enque(page);
	}
	
	public static void asyncWritePage(Page page){
		writeContext.enque(page);
	}
	
	public static void asyncDispatch(Store storeCallback){
//	public static void asyncDispatch(StoreCallback storeCallback){
		dispatchContext.enque(storeCallback);
	}
	
	private static StoreFile getBufferFile(int fileId){
		return bufferFiles[fileId];
	}
	
	/**
	 * ���ݐ����Ă���persistenceStore��storeId��񋓂���
	 * @return
	 */
	public static Set<Long> listPersistenceStoreId(){
		return persistenceStore.listPersistenceStoreId();
	}

	/**
	 * store�̎Q�Ƃ𑝂₷
	 * @param digest
	 * @return
	 */
	public static boolean ref(String digest){
		if(digest==null){
			return false;
		}
		return persistenceStore.ref(digest);
	}
	
	/**
	 * store�̎Q�Ƃ����炷
	 * @param digest
	 * @return
	 */
	public static boolean unref(String digest){
		if(digest==null){
			return false;
		}
		return persistenceStore.unref(digest);
	}
	
	/**
	 * storeId���炻��store�̃f�[�^����ԋp����
	 * �R���e�i�ォ��g�p�ł���
	 * @param storeId
	 * @return store�̃f�[�^��
	 */
	public static long getStoreLength(long storeId){
		return persistenceStore.getPersistenceStoreLength(storeId);
	}

	public static long getStoreLength(String digest){
		return persistenceStore.getPersistenceStoreLength(digest);
	}
	
	public static String getStoreDigest(long storeId){
		return persistenceStore.getPersistenceStoreDigest(storeId);
	}
	
	public static long getStoreId(String digest){
		return persistenceStore.getPersistenceStoreId(digest);
	}
	
	public static int getStoreRefCount(long storeId){
		if(storeId==Store.FREE_ID){
			return -1;
		}
		return persistenceStore.getPersistenceStoreRefCount(storeId);
	}
	public static int getStoreRefCount(String digest){
		return getStoreRefCount(getStoreId(digest));
	}
	
	public static void removeStore(long storeId){
		persistenceStore.remove(storeId);
	}
	
	/*
	public static void removeStore(String digest){
		persistenceStore.unref(digest);
	}
	*/
	
	public static boolean recoverStore(){
		return checkStore(true);
	}
	
	public static boolean checkStore(){
		return checkStore(false);
	}
	
	private static class DigestChecker implements BufferGetter{
		private boolean isRecover;
		private int failCount;
		private int okCount;
		
		DigestChecker(boolean isRecover){
			this.isRecover=isRecover;
		}
		private synchronized void endCheck(boolean result){
			if(result){
				okCount++;
			}else{
				failCount++;
			}
			notify();
		}
		
		private synchronized boolean waitCheck(int digestCheckCount){
			while(true){
				if(digestCheckCount<=(okCount+failCount)){
					break;
				}
				try {
					wait();
				} catch (InterruptedException ignore) {
				}
			}
			if(failCount>0){
				return false;
			}
			return true;
		}
		
		public boolean onBuffer(Object userContext, ByteBuffer[] buffers) {
			Store store=(Store)userContext;
			MessageDigest messageDigest=store.getMessageDigest();//messageDigest�̏���
			for(ByteBuffer buffer:buffers){
				ByteBuffer buf=buffer;
				int pos=buf.position();
				int len=buf.limit()-pos;
				messageDigest.update(buf.array(),pos,len);
			}
			return true;//�p�����ēǂ�
		}
		public void onBufferEnd(Object userContext) {
			Store store=(Store)userContext;
			MessageDigest messageDigest=store.getMessageDigest();//messageDigest�̏���
			String digest=DataUtil.digest(messageDigest);
			String expectDigest=getStoreDigest(store.getStoreId());
			boolean result=digest.equals(expectDigest);
			logger.warn("checkStore digest check fail.storeId:"+store.getStoreId());
			if(result==false && isRecover){
				logger.warn("recoverStore.storeId:"+store.getStoreId());
				removeStore(store.getStoreId());
			}
			endCheck(result);
		}
		public void onBufferFailure(Object userContext, Throwable failure) {
			Store store=(Store)userContext;
			if(isRecover){
				removeStore(store.getStoreId());
			}
			endCheck(false);
		}
	}
	
	public static boolean checkStore(boolean isRecover){
		boolean openResult=true;
		DigestChecker digestChecker=new DigestChecker(isRecover);
		int digestCheckCount=0;
		Set<Long> ids=StoreManager.listPersistenceStoreId();
		Iterator<Long> itr=ids.iterator();
		itr=ids.iterator();
		while(itr.hasNext()){
			long storeId=itr.next();
			Store store=Store.open(storeId);
			if(store==null){
				logger.warn("checkStore open check fail.storeId:"+storeId);
				if(isRecover){
					logger.warn("recoverStore.storeId:"+storeId);
					removeStore(storeId);
				}
				openResult=false;
				continue;
			}
			store.asyncBuffer(digestChecker, store);
			digestCheckCount++;
		}
		boolean digestCheckerResult=digestChecker.waitCheck(digestCheckCount);
		return digestCheckerResult&&openResult;
	}
	
	public static void dumpStore() throws IOException{
		Set<Long> ids=StoreManager.listPersistenceStoreId();
		StoreArchiver.toArchive(ids, dumpFile);
	}
	
	/**
	 * HashLock�֘A
	 * storeId���L�[�Ƀ��b�N�𕡐������ĕ��U����
	 * 1)Page�̓ǂݍ��݁A�������݂��o�b�e�B���O���Ȃ��悤�ɂ���
	 * 2)load����Page(pageFile����ǂݍ��񂾂�pageIn��)������Ԃ�compress���Ȃ��悤�ɂ���
	 */
	private static HashLock[] hashLocks;
	static{
		hashLocks=new HashLock[HASH_LOCK_INDEX];
		for(int i=0;i<hashLocks.length;i++){
			hashLocks[i]=new HashLock();
		}
	}
	private static class HashLock{
		private int[] fileidPageInCounter=new int[MAX_FILE_ID];
	}
	private static HashLock getHashLock(long storeId){
		return hashLocks[((int)storeId)%HASH_LOCK_INDEX];
	}
	
	public static Page preparePageIn(Store store,long pageId){
		HashLock hashLock=getHashLock(store.getStoreId());
		synchronized(hashLock){
			Page page=Page.loadPage(store,pageId);
			if(page==null){
				return null;
			}
			hashLock.fileidPageInCounter[page.getFileId()]++;
			return page;
		}
	}
	
	public static Page preparePageIn(Page page){
		HashLock hashLock=getHashLock(page.getStoreId());
		synchronized(hashLock){
			hashLock.fileidPageInCounter[page.getFileId()]++;
			return page;
		}
	}
	
	public static void donePageIn(Page page){
		HashLock hashLock=getHashLock(page.getStoreId());
		synchronized(hashLock){
			hashLock.fileidPageInCounter[page.getFileId()]--;
		}
	}
	
	private static void compressSavePage(Page page){
		HashLock hashLock=getHashLock(page.getStoreId());
		synchronized(hashLock){
			page.save();
		}
	}
	
	/**
	 * pageIn�r����Page�̗L����ԋp
	 * @param fileId
	 * @return
	 */
	public static boolean checkPageIn(int fileId){
		for(int i=0;i<hashLocks.length;i++){
			synchronized(hashLocks[i]){
				if(hashLocks[i].fileidPageInCounter[fileId]!=0){
					return true;
				}
			}
		}
		return false;
	}
	
	public static StoreStastics getStoreStastics(){
		return storeStastics;
	}
	static String infoStoreStastics(){
		return storeStastics.info();
	}
	static synchronized void countStoreStastics(Store store){
		storeStastics.countStore(store);
	}
	
	public static class Dispatcher implements Queuelet,Timer{
		private Object interval;
		private StoreFile compressBufferFile=null;
		private Object compressLock=new Object();
		private boolean isEnd=false;
		private boolean isTimerCompressing=false;
		
		private void moveBuffer(Page page) throws IOException{
			page.fillBuffer(compressBufferFile);
			Integer fileId=null;
			try {
				fileId = writeFileIds.take();
			} catch (InterruptedException e) {
				throw new IllegalStateException("writeFileIds.take() error.",e);
			}
			try{
				page.flushBuffer(fileId.intValue(), bufferFiles[fileId.intValue()]);
			}finally{
				writeFileIds.offer(fileId);
			}
			//���̊Ԃ�hashLock�ɂ�蓯���ǂݍ��݂�j�~
			compressSavePage(page);
		}
		
		private void compressPage(int compressFileId) throws IOException{
			logger.debug("compress.start.compressBufferId:"+compressFileId);
			Page compressPage=null;
			long pageId=Page.FREE_ID;
			while(true){
				compressPage=Page.nextCompressPage(pageId);
				if(compressPage==null){
					break;
				}
				synchronized(compressLock){//�I���R�}���h���󂯕t�����瑦���ɕ��A����
					if(isEnd){
						logger.debug("compress recive end request.compressBufferId:"+compressFileId);
						return;
					}
				}
				pageId=compressPage.getPageId();
				long storeId=compressPage.getStoreId();
				if(Store.isLiveStoreId(storeId)==false){
					compressPage.free(true,true);//pageFile���FREE_ID�ɐݒ�
					continue;
				}
				//�L����Page
				if(compressPage.getFileId()!=compressFileId){
					compressPage.free(false);//compress�Ώۂ���Ȃ�
					continue;
				}
				moveBuffer(compressPage);
				compressPage.free(false);//compress�ς�
			}
			bufferFiles[compressFileId].truncate();
			compressFileId++;
			if( compressFileId>=bufferFiles.length){
				compressFileId=0;
			}
			persistenceStore.setCompressFileId(compressFileId);
			logger.debug("compress.end.compressBufferId:"+compressFileId);
		}
		
		//���������������Ƃ�O��ŋ����I��compress����
		private void forceCompress(){
			if(!persistenceStore.checkAndStartCompress()){
				//compress�̕K�v�Ȃ�
				return;
			}
			for(int compressFileId=0;compressFileId<bufferFiles.length;compressFileId++){
				compressBufferFile(compressFileId);
			}
		}
		
		//�w�肳�ꂽbufferFile��compress����
		private void compressBufferFile(int compressFileId){
			compressBufferFile=bufferFiles[compressFileId];
			//�������ݒS����writeFileIds�����U�[�u����compress���������܂�Ȃ��悤�ɂ���
			Integer fileId=null;
			while(true){
				try {
					fileId=writeFileIds.take();
				} catch (InterruptedException e) {
					logger.error("fail to doCompress.",e);
					return;
				}
				if(fileId.intValue()==compressFileId){
					break;
				}
				writeFileIds.offer(fileId);
			}
			logger.info("compress start.compressFileId:"+compressFileId);
			storeStastics.setCompressFileId(compressFileId);
			try {
				compressPage(compressFileId);
			} catch (Exception e) {
				logger.error("fail to compressBuffer.",e);
			}finally{
				logger.info("compress end.compressFileId:"+compressFileId);
				writeFileIds.offer(fileId);
				storeStastics.setCompressFileId(-1);
			}
		}
		
		private void doTimerCompress(){
			int compressFileId=persistenceStore.getCompressFileId();
			if(compressFileId==0){
				if(persistenceStore.checkAndStartCompress()==false){
					return;//�K�v�������ꍇ��compress���Ȃ�
				}
			}
			//compressBufferFile(compressFileId);
		}
		
		public void init(QueueletContext context, Map param) {
			logger.info("Dispatcher init");
			StoreManager.dispatchContext=context;
			//�N���O��Store��compress����,�K�{�ł͂Ȃ���
			if("true".equals(param.get("initCompress"))){
				System.out.println("compless store start");
				forceCompress();
				logger.info("compress store end");
				System.out.println("compless store end");
			}
			infoStastics();
			interval=TimerManager.setInterval(compressInterval, this, "StoreManagerTimer");
		}
		
		public void term() {
			infoStastics();
			TimerManager.clearInterval(interval);
			//compress���͏I���������Ⴂ���Ȃ�
			synchronized(compressLock){
				isEnd=true;
				while(isTimerCompressing){
					try {
						compressLock.wait();
					} catch (InterruptedException e) {
					}
				}
			}
			logger.info("Dispatcher trem");
		}
		
		public boolean service(Object req) {
			//����worker�������ꍇ�Aput����callback����Ȃ��\��������B
			Store store=(Store)req;
			store.callback();
			storeStastics.countCallback();
			return false;
		}
		
		private void infoStastics(){
			//����,pageOut��,pageOut�T�C�Y,pageIn��,pageIn�T�C�Y,callback��,persistStore��,plainStore��
			String stasticsCsv=storeStastics.csvInfo();
			StringBuilder sb=new StringBuilder("store stastics,");
			sb.append(System.currentTimeMillis());
			sb.append(",");
			sb.append(stasticsCsv);
			logger.info(sb.toString());
		}
		long lastCallbackCount=0;
		//Store���R���v���X�A�����e���邽�߂̃C�x���g
		public void onTimer(Object userContext) {
			long callbackCount=storeStastics.getCallbackCount();
			logger.debug("callbackCount:"+callbackCount);
			//timer�Ԋu�̊Ԃ�callback���ŕ��חʂ𐄑��A�ɂȂƂ���compress��persistenceStore.save����
			if(callbackCount==lastCallbackCount){
				logger.debug("persistenceStore.save() lastCallbackCount:"+lastCallbackCount);
				persistenceStore.save(false);
				logger.debug("persistenceStore.save() return");
			}
			lastCallbackCount=callbackCount;
//			infoStastics();
			Page.saveFreePage();
			synchronized(compressLock){
				if(isEnd){
					return;
				}
				isTimerCompressing=true;
			}
			doTimerCompress();
			synchronized(compressLock){
				isTimerCompressing=false;
				compressLock.notify();
			}
		}
	}
	
	public static class BufferFileReader implements Queuelet{
		public void init(QueueletContext context, Map param) {
			StoreManager.readContext=context;
		}
		public void term() {
			logger.info("BufferFileReader trem");
		}
		public boolean service(Object req) {
			Page page=(Page)req;
			Throwable failure=null;
			int fileId=page.getFileId();
			if(fileId<0){//pageIn���������邷��O��pageOut�v��������
				page.onPageIn();
				return false;
			}
			long size=0;
			try {
				size=page.getBufferLength();
				page.fillBuffer(getBufferFile(fileId));
			} catch (IOException e) {
				logger.error("page.fillBuffer error.",e);
				failure=e;
			} catch (Throwable e) {
				logger.error("page.fillBuffer Throwable error.",e);
				failure=e;
			}
			if(failure!=null){
				page.onFailure(failure);
			}else{
				page.onPageIn();
				storeStastics.countPageIn(size);
			}
			return false;
		}
	}
	
	public static class BufferFileWriter implements Queuelet{
		private File persistenceStoreFile;
		
		public void init(QueueletContext context, Map param) {
			try {
				boolean isCleanup=false;
				String args[]=(String[])param.get("QueueletArgs");
				for(String arg:args){
					if("cleanup".equalsIgnoreCase(arg)){
						System.out.println("StoreManager recive cleanup");
						isCleanup=true;
					}
				}
				StoreManager.writeContext=context;
				
				String compressIntervalParam=(String)param.get("compressInterval");
				if(compressIntervalParam!=null){
					compressInterval=Long.parseLong(compressIntervalParam);
				}
				logger.info("compressInterval:"+compressInterval);

				persistenceStoreFile=new File((String)param.get("persistenceStore.file"));
				dumpFile=new File(persistenceStoreFile.getParent(),DUMP_FILE_NAME);
				if(persistenceStoreFile.exists()){
					if(isCleanup){
						persistenceStoreFile.delete();
					}
				}else{
					isCleanup=true;//persistenceStore.file�������ꍇ�A���̑��̃t�@�C�����������v
				}
				persistenceStore=PersistenceStore.load(persistenceStoreFile);
				//Store�̏�����
				Store.init(persistenceStore);
				
				//Page�̏�����
				String pageFileName=(String)param.get("page.file");
				File pageFile=new File(pageFileName);
				if(isCleanup&&pageFile.exists()){
					pageFile.delete();
				}
				String pageReaderCountParam=(String)param.get("page.readerCount");
				int pageReaderCount=Integer.parseInt(pageReaderCountParam);
				StoreFile pageStoreFile=new StoreFile(pageFile,pageReaderCount);
				
				Page.init(persistenceStore,pageStoreFile);
				
				//Buffer�̍쐬
				String bufferReaderCountParam=(String)param.get("buffer.readerCount");
				int bufferReaderCount=Integer.parseInt(bufferReaderCountParam);
				
				Map<Integer,StoreFile> bufFiles=new HashMap<Integer,StoreFile>();
				int bufFileCount=0;
				for(bufFileCount=0;;bufFileCount++){
					String bufferFileName=(String)param.get("buffer." +bufFileCount + ".file");
					if(bufferFileName==null){
						break;
					}
					File bufferFile=new File(bufferFileName);
					if(isCleanup&&bufferFile.exists()){
						bufferFile.delete();
					}
					bufFiles.put(bufFileCount, new StoreFile(bufferFile,bufferReaderCount));
				}
				bufferFiles=new StoreFile[bufFileCount];
				writeFileIds=new ArrayBlockingQueue<Integer>(bufFileCount);
				for(int i=0;i<bufFileCount;i++){
					bufferFiles[i]=bufFiles.get(i);
					writeFileIds.offer(i);
				}
				
				//���v���̏�����
				storeStastics=new StoreStastics(persistenceStore,bufferFiles);
			} catch (Exception e) {
				logger.error("init error.",e);
				throw new IllegalStateException("init error.",e);
			}
		}
		
		public void term() {
			logger.info("BufferFileWriter trem");
			for(int i=0;i<bufferFiles.length;i++){
				writeFileIds.poll();
			}
			for(int i=0;i<bufferFiles.length;i++){
				bufferFiles[i].close();
			}
			Page.term();
			Store.term();
			persistenceStore.save(true);
		}
		
		public boolean service(Object req) {
			Page page=(Page)req;
			Integer fileId=null;
			Throwable failure=null;
			long size=0;
			try {
				fileId=writeFileIds.take();
				size=page.getBufferLength();
				page.flushBuffer(fileId, getBufferFile(fileId));
			} catch (InterruptedException e) {
				logger.error("take error.",e);
				failure=e;
			} catch (IOException e) {
				logger.error("page.fillBuffer error.",e);
				failure=e;
			} catch (Throwable e) {
				logger.error("page.fillBuffer Throwable error.",e);
				failure=e;
			} finally{
				if(fileId!=null){
					writeFileIds.offer(fileId);
				}
			}
			if(failure!=null){
				page.onFailure(failure);
			}else{
				page.onPageOut();
				storeStastics.countPageOut(size);
			}
			return false;
		}
	}
}
