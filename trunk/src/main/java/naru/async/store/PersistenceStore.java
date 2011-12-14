package naru.async.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class PersistenceStore implements Serializable {
	private static Logger logger=Logger.getLogger(StoreManager.class);
	private static final long serialVersionUID = -2006116447875471224L;
	
	private File persistenceStoreFile;
	
	public static PersistenceStore load(File persistenceStoreFile) throws IOException{
		InputStream is=null;
		if(persistenceStoreFile.exists()){
			is=new FileInputStream(persistenceStoreFile);
		}
		PersistenceStore instance=PersistenceStore.load(is);
		instance.persistenceStoreFile=persistenceStoreFile;
		instance.isUpdate=false;
		if(is!=null){
			is.close();
		}
		return instance;
	}
	

	private static PersistenceStore load(InputStream is) throws IOException{
		if(is==null){
			PersistenceStore p=new PersistenceStore();
			p.topFreePageId=Page.FREE_ID;
			p.storeIdMap=new HashMap<Long,StoreEntry>();
			p.pageIdMap=new HashMap<Long,StoreEntry>();
			p.digestMap=new HashMap<String,StoreEntry>();
			p.isNomalEnd=true;
			return p;
		}
		ObjectInputStream ois=new ObjectInputStream(is);
		try {
			PersistenceStore p=(PersistenceStore)ois.readObject();
			p.pageIdMap=new HashMap<Long,StoreEntry>();
			p.digestMap=new HashMap<String,StoreEntry>();
			for(StoreEntry se:p.storeIdMap.values()){
				p.pageIdMap.put(se.getTopPageId(), se);
				p.digestMap.put(se.getDigest(), se);
			}
			return p;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException("PersistenceStore laod error.",e);
		}
	}
	
	public void save(boolean isTerm){
		synchronized(this){
			if(isTerm==false && isUpdate==false){
				return;
			}
			isUpdate=false;
		}
		logger.debug("save() start");
		this.isNomalEnd=isTerm;
		OutputStream os=null;
		try {
			os=new FileOutputStream(persistenceStoreFile);
			ObjectOutputStream oos=new ObjectOutputStream(os);
			synchronized(this){
				oos.writeObject(this);
			}
		}catch (IOException e) {
			logger.error("persistenceStore save eroor",e);
		}finally{
			logger.debug("save() end");
			if(os!=null){
				try {
					os.close();
				}catch(IOException ignore){
				}
			}
		}
	}
	
	/* saveしてからの更新の有無 */
	private boolean isUpdate=false;

	/* 終了処理をしたか否か */
	private boolean isNomalEnd;
	
	//Page管理用の保存値
	private long pageIdSequence;
	private long topFreePageId;
	private int compressFileId;
	
	private long storeIdSequence;
	private Map<Long,StoreEntry> storeIdMap;
	private transient Map<Long,StoreEntry> pageIdMap;
	private transient Map<String,StoreEntry> digestMap;
	
	private long garbageSize;
	
	public void addGarbageSize(long garbageSize){
		this.garbageSize+=garbageSize;
	}
	
	public boolean checkAndStartCompress(){
		if(garbageSize==0){
			return false;
		}
		logger.info("needCompress.garbageSize:"+garbageSize);
		garbageSize=0;
		return true;
	}
	
	long getGarbageSize(){
		return garbageSize;
	}
	
	long nextPageId(){
		long next=pageIdSequence*Page.PAGE_SIZE;
		pageIdSequence++;
		return next;
	}
	
	long getPageIdMax(){
		return pageIdSequence*Page.PAGE_SIZE;
	}
	
	public long getTopFreePageId(){
		return topFreePageId;
	}
	
	public void setTopFreePageId(long topFreePageId){
		this.topFreePageId=topFreePageId;
	}
	
	public int getCompressFileId() {
		return compressFileId;
	}

	public void setCompressFileId(int compressFileId) {
		this.compressFileId = compressFileId;
	}
	
	int getPersistenceStoresCount(){
		return storeIdMap.size();
	}
	synchronized Set<Long> listPersistenceStoreId(){
		//cloneしてこのsnapshotを返却
		return new HashSet<Long>(storeIdMap.keySet());
	}
	
	long getPersistenceStoreLength(long storeId){
		StoreEntry se=getStoreByStoreId(storeId);
		if(se==null){
			return -1;
		}
		return se.length;
	}
	
	long getPersistenceStoreLength(String digest){
		StoreEntry se=getStoreByDigest(digest);
		if(se==null){
			return -1;
		}
		return se.length;
	}
	
	String getPersistenceStoreDigest(long storeId){
		StoreEntry se=getStoreByStoreId(storeId);
		if(se==null){
			return null;
		}
		return se.getDigest();
	}
	
	long getPersistenceStoreId(String digest){
		StoreEntry se=getStoreByDigest(digest);
		if(se==null){
			return Store.FREE_ID;
		}
		return se.getStoreId();
	}
	
	int getPersistenceStoreRefCount(long storeId){
		StoreEntry se=getStoreByStoreId(storeId);
		if(se==null){
			return -1;
		}
		return se.getRefCount();
	}
	
	public static class StoreEntry implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = -4289260506137503467L;
		
		StoreEntry(long storeId,long topPageId,long length,String digest){
			this.storeId=storeId;
			this.topPageId=topPageId;
			this.length=length;
			this.digest=digest;
			this.refCount=1;
		}
		private long storeId;
		private long topPageId;
		private long length;
		private String digest; //データダイジェスト(25byte)
		private int refCount; //参照数,当面使わない
		
		private transient Set<Store> stores;//open中のStore群
		public long getStoreId() {
			return storeId;
		}
		public long getTopPageId() {
			return topPageId;
		}
		public long getLength() {
			return length;
		}
		public synchronized void addStore(Store store){
			if(stores==null){
				stores=new HashSet<Store>();
			}
			stores.add(store);
		}
		public synchronized void delStore(Store store){
			stores.remove(store);
		}
		public String getDigest() {
			return digest;
		}
		public void setDigest(String digest) {
			this.digest = digest;
		}
		public int getRefCount() {
			return refCount;
		}
		public void setRefCount(int refCount) {
			this.refCount = refCount;
		}
	}
	
	public synchronized boolean ref(String digest){
		StoreEntry storeEntry=digestMap.get(digest);
		if(storeEntry!=null){
			storeEntry.refCount++;
			return true;
		}
		return false;
	}
	public synchronized boolean unref(String digest){
		StoreEntry storeEntry=digestMap.get(digest);
		if(storeEntry!=null){
			storeEntry.refCount--;
			if(storeEntry.refCount!=0){
				return true;
			}
			//必要なくなったBufferサイズを加算
			addGarbageSize(storeEntry.getLength());
			pageIdMap.remove(storeEntry.getTopPageId());
			digestMap.remove(storeEntry.getDigest());
			return true;
		}
		return false;
	}
	
	/**
	 * digest値が等しいStoreが既にあれば、そのstoreIdを返却、今まで使っていたstoreIdは捨て
	 * @param storeId
	 * @param topPageId
	 * @param length
	 * @param digest
	 */
	public synchronized long add(long storeId,long topPageId,long length,String digest){
		isUpdate=true;
		StoreEntry storeEntry=digestMap.get(digest);
		if(storeEntry!=null){
			storeEntry.refCount++;
			return storeEntry.storeId;
		}
		storeEntry=new StoreEntry(storeId,topPageId,length,digest);
		storeIdMap.put(storeId, storeEntry);
		pageIdMap.put(topPageId, storeEntry);
		digestMap.put(digest, storeEntry);
		return storeId;
	}
	
	public synchronized void remove(long storeId){
		isUpdate=true;
		StoreEntry storeEntry=storeIdMap.remove(storeId);
		if(storeEntry==null){
			return;
		}
		storeEntry.refCount--;
		if(storeEntry.refCount!=0){
			return;
		}
		//必要なくなったBufferサイズを加算
		addGarbageSize(storeEntry.getLength());
		pageIdMap.remove(storeEntry.getTopPageId());
		digestMap.remove(storeEntry.getDigest());
	}
	
	public synchronized StoreEntry getStoreByStoreId(long storeId){
		return storeIdMap.get(storeId);
	}
	
	public synchronized StoreEntry getStoreByPageId(long pageId){
		return pageIdMap.get(pageId);
	}
	
	public synchronized StoreEntry getStoreByDigest(String digest){
		return digestMap.get(digest);
	}
	
	public synchronized long getStoreIdMax(){
		return storeIdSequence;
	}
	
	public synchronized long nextStoreId(){
		storeIdSequence++;
		return storeIdSequence;
	}


	public boolean isNomalEnd() {
		return isNomalEnd;
	}

}
