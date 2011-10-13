package naru.async.store;

public class StoreStastics {
	private PersistenceStore persistenceStore;
	private StoreFile[] bufferFiles;
	
	private int compressFileId;
	
	private int putStoreCount;
	private int getStoreCount;
	private int putgetStoreCount;
	
	private int putBufferCount;
	private long putBufferLength;
	private int onBufferCount;
	private long onBufferLength;
	private int onBufferEndCount;
	private int onBufferFailureCount;
	
	//swapin��,size swapout��,size,callback��
	private int callbackCount=0;
	private int pageInCount=0;
	private int pageOutCount=0;
	private long pageInSize;
	private long pageOutSize;
	
	StoreStastics(PersistenceStore persistenceStore,StoreFile[] bufferFiles){
		this.persistenceStore=persistenceStore;
		this.bufferFiles=bufferFiles;
	}
	
	void recycleStore(){
		putBufferCount=onBufferCount=onBufferEndCount=onBufferFailureCount=0;
		putBufferLength=onBufferLength=0L;
	}
	
	//compress fileid �J�n����
	public String getCompressBufferName(){
		return bufferFiles[0].toString();
	}
	
	public int getBufferFileCount(){
		return bufferFiles.length;
	}
	
	//fileid����Buffer�t�@�C���T�C�Y
	public long getBufferFileSize(int fileId){
		if(fileId<0 || fileId>=bufferFiles.length){
			return -1;
		}
		return bufferFiles[fileId].length();
	}
	
	public long getBufferFileSize(){
		long total=0;
		for(int fileId=0;fileId<bufferFiles.length;fileId++){
			total+=bufferFiles[fileId].length();
		}
		return total;
	}
	
	//TODO ���v���(���v���̂��߂ɔr���͎��Ȃ��A���̂��ߎ኱�̌덷�͗e�F)
	//PlainStore���APersistenceStore��,storeIdMax
	public long getStoreIdMax(){
		return persistenceStore.getStoreIdMax();
	}
	public int getPlainStoreCount(){
		return Store.getPlainStoresCount();
	}
	public int getPersistenceStoresCount(){
		return persistenceStore.getPersistenceStoresCount();
	}
	
	//FreePage���APlainPage���ApageIdMax
	public int getFreePageCount(){
		return Page.getFreePageCount();
	}
	public int getPlainPageCount(){
		return Page.getPlainPageCount();
	}
	public long getPageIdMax(){
		return persistenceStore.getPageIdMax();
	}

	public long getGarbageSize(){
		return persistenceStore.getGarbageSize();
	}
	
	/**
	 * store�I�����ɂ���store�̓��v�������Z����
	 * 
	 * @param s
	 * @param kind 
	 * @return
	 */
	void countStore(Store store){
		switch(store.getKind()){
		case GET:
			getStoreCount++;
			break;
		case PUT:
			putStoreCount++;
			break;
		case PUTGET:
			putgetStoreCount++;
			break;
		}
		putBufferCount+=store.getPutBufferCount();
		putBufferLength+=store.getPutBufferLength();
		onBufferCount+=store.getOnBufferCount();
		onBufferLength+=store.getOnBufferLength();
		onBufferEndCount+=store.getOnBufferEndCount();
		onBufferFailureCount+=store.getOnBufferFailureCount();
	}
	
	void countPutBuffer(long length){
		putBufferCount++;
		putBufferLength+=length;
	}
	
	//���v�֐�,StoreCallback������Z�����A�r�����ɌĂяo�����
	void countCallbackBuffer(long length){
		onBufferCount++;
		onBufferLength+=length;
	}
	void countCallbackEnd(){
		onBufferEndCount++;
		
	}
	void countCallbackFailure(){
		onBufferFailureCount++;
	}
	
	void countPageIn(long length){
		pageInCount++;
		pageInSize+=length;
	}
	
	void countPageOut(long length){
		pageOutCount++;
		pageOutSize+=length;
	}
	
	void countCallback(){
		callbackCount++;
	}
	
	String csvInfo(){
		StringBuilder sb=new StringBuilder();
		sb.append(getStoreCount);
		sb.append(",");
		sb.append(putStoreCount);
		sb.append(",");
		sb.append(putgetStoreCount);
		sb.append(",#,");
		
		sb.append(putBufferCount);
		sb.append(",");
		sb.append(putBufferLength);
		sb.append(",");
		sb.append(onBufferCount);
		sb.append(",");
		sb.append(onBufferLength);
		sb.append(",");
		sb.append(onBufferEndCount);
		sb.append(",");
		sb.append(onBufferFailureCount);
		sb.append(",#,");
		
		sb.append(pageOutCount);
		sb.append(",");
		sb.append(pageOutSize);
		sb.append(",");
		sb.append(pageInCount);
		sb.append(",");
		sb.append(pageInSize);
		sb.append(",");
		sb.append(callbackCount);

		sb.append(",#,");
		
		sb.append(getPersistenceStoresCount());
		sb.append(",");
		sb.append(getPlainStoreCount());
		sb.append(",");
		sb.append(getStoreIdMax());
		
		sb.append(",#,");
		sb.append(persistenceStore.getCompressFileId());
		sb.append(",");
		sb.append(persistenceStore.getGarbageSize());
		for(int i=0;i<bufferFiles.length;i++){
			StoreFile storeFile=bufferFiles[i];
			sb.append(",");
			sb.append(i);
			sb.append(",");
			sb.append(storeFile.length());
		}
		return sb.toString();
	}
	
	String info(){
		StringBuilder sb=new StringBuilder();
		sb.append("getStoreCount:");
		sb.append(getStoreCount);
		sb.append(":putStoreCount:");
		sb.append(putStoreCount);
		sb.append(":putgetStoreCount:");
		sb.append(putgetStoreCount);
		sb.append(":putBufferCount:");
		sb.append(putBufferCount);
		sb.append(":putBufferLength:");
		sb.append(putBufferLength);
		sb.append(":onBufferCount:");
		sb.append(onBufferCount);
		sb.append(":onBufferLength:");
		sb.append(onBufferLength);
		sb.append(":onBufferEndCount:");
		sb.append(onBufferEndCount);
		sb.append(":onBufferFailureCount:");
		sb.append(onBufferFailureCount);
		sb.append(":pageOutCount:");
		sb.append(pageOutCount);
		sb.append(":pageOutSize:");
		sb.append(pageOutSize);
		sb.append(":pageInCount:");
		sb.append(pageInCount);
		sb.append(":pageInSize:");
		sb.append(pageInSize);
		sb.append(":callbackCount:");
		sb.append(callbackCount);
		sb.append(":compressFileId:");
		sb.append(persistenceStore.getCompressFileId());
		sb.append(":garbageSize:");
		sb.append(persistenceStore.getGarbageSize());
		for(int i=0;i<bufferFiles.length;i++){
			StoreFile storeFile=bufferFiles[i];
			sb.append(":buffer.");
			sb.append(i);
			sb.append(":"+storeFile.length());
		}
		return sb.toString();
	}

	public int getCompressFileId() {
		return compressFileId;
	}

	public void setCompressFileId(int compressFileId) {
		this.compressFileId = compressFileId;
	}

	public int getCallbackCount() {
		return callbackCount;
	}

	public int getPageInCount() {
		return pageInCount;
	}

	public int getPageOutCount() {
		return pageOutCount;
	}

	public long getPageInSize() {
		return pageInSize;
	}

	public long getPageOutSize() {
		return pageOutSize;
	}
}
