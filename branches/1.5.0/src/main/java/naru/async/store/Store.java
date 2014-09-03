package naru.async.store;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;

import naru.async.BufferGetter;
import naru.async.Log;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.async.store.StoreCallback;
import naru.async.store.PersistenceStore.StoreEntry;

public class Store extends PoolBase {
	private static Logger logger=Logger.getLogger(Store.class);
	//persistenceStore�Q���Ǘ�����(����stoerId�ɑ΂��ĕ���open����ꍇ������)
	private static PersistenceStore persistenceStore;
	public static final long FREE_ID=-1;
	
	public static void init(PersistenceStore persistenceStore){
		Store.persistenceStore=persistenceStore;
	}
	public static void term(){
	}
	
	//PUT�Ŗ�close,PUTGET��Store�Q���Ǘ�����(stoerId�ň�ӂɎ��ʂł���)
	private static Map<Long,Store> plainStores=new HashMap<Long,Store>();
	
	static boolean isLiveStoreId(long storeId){
		if(plainStores.containsKey(storeId)){
			return true;
		}
		if(persistenceStore.getStoreByStoreId(storeId)!=null){
			return true;
		}
		return false;
	}
	static int getPlainStoresCount(){
		return plainStores.size();
	}
	
	private synchronized static void register(Store store){
		plainStores.put(store.getStoreId(), store);
	}
	private synchronized static void deregister(long storeId){
		plainStores.remove(storeId);
	}
	
	public enum Kind {
		PUTGET,
		GET,
		PUT,
	}
	private Page gettingPage;//get�Ώ�Page
	private Page puttingPage;//put�Ώ�Page
	private Map<Long,Page> savingPages=new HashMap<Long,Page>();//save��Page
	private Page loadingPage;//load��Page
	private Throwable failure;//���u���ɔ�������error
	
	private Kind kind;
	private long storeId=-1;
	private long topPageId;//�擪Page��pageId,PUT,GET�̏ꍇ�L���APUTGET�̏ꍇ-1
	private String digest;
	private long pageOutSumLength;//pageOut�������v�T�C�Y,compress����Ɏg�p
	private long putLength;
	private long getLength;
	private boolean isFin=false;//��close���Ă�(unref�ς݁j
	private boolean isCloseWait=false;//asyncClose���󂯕t����
	private boolean isClosed=false;//close���󂯕t����
	private boolean isClosePersistance=false;//close��t���Ɏw�肳�ꂽ�I�v�V����
	private boolean isPutCloseStoreId=false;//put�@close�� storeId���Čv�Z�������ۂ�
	
	private boolean isOnAsyncBuffer;//callback������
	private boolean isOnAsyncBufferRequest;//callback��������asyncRead����
	private boolean isOnAsyncBufferClose;//callback��������close����
	private BufferGetter bufferGetter;//asyncBuffer����
	private Object userContext;
	
	private MessageDigest messageDigest;//=MessageDigest.getInstance("MD5");
//	private StoreStastices stastics=new StoreStastices();
	
	//���v���
	private int putBufferCount;
	private long putBufferLength;
	private int onBufferCount;
	private long onBufferLength;
	private int onBufferEndCount;
	private int onBufferFailureCount;
	
	@Override
	public void recycle() {
		isFin=false;
		isClosed=false;
		isCloseWait=false;
		isClosePersistance=false;
		isPutCloseStoreId=false;
		isCallbackProcessing=false;
		
		isOnAsyncBuffer=isOnAsyncBufferRequest=isOnAsyncBufferClose=false;
		bufferGetter=null;
		
		loadingPage=null;
		savingPages.clear();
//		stastics.recycleStore();
		putBufferCount=onBufferCount=onBufferEndCount=onBufferFailureCount=0;
		putBufferLength=onBufferLength=0L;
		
		if(messageDigest!=null){
			messageDigest.reset();
		}
		failure=null;
		pageOutSumLength=0;
		super.recycle();
	}
	
	public static Store open(boolean isPersistence){
		Store store=(Store)PoolManager.getInstance(Store.class);
		store.storeId=persistenceStore.nextStoreId();
		store.putLength=store.getLength=0;
		store.puttingPage=Page.allocPage(store,null);
		store.topPageId=store.puttingPage.getPageId();
		if(isPersistence){//�ۑ���p
			store.getMessageDigest();//messageDigest�̏���
			store.kind=Kind.PUT;
			store.gettingPage=null;
		}else{//�ǂݏ����o�b�t�@
			store.kind=Kind.PUTGET;
			store.gettingPage=store.puttingPage;
		}
		register(store);
		Log.debug(logger,"open.isPersistence:",isPersistence,":sid:",store.getStoreId(),":this:",store);
		return store;
	}
	public static Store open(String digest){
		StoreEntry se=persistenceStore.getStoreByDigest(digest);
		if(se==null){
			return null;
		}
		return open(se);
	}
	
	public static Store open(long storeId){
		StoreEntry se=persistenceStore.getStoreByStoreId(storeId);
		if(se==null){
			return null;
		}
		return open(se);
	}
	
	private static Store open(StoreEntry se){
		Store store=(Store)PoolManager.getInstance(Store.class);
		store.kind=Kind.GET;
		store.storeId=se.getStoreId();
		store.gettingPage=null;
		store.puttingPage=null;
		store.topPageId=se.getTopPageId();
		store.loadingPage=StoreManager.preparePageIn(store,store.topPageId);
		if(store.loadingPage==null){
			PoolManager.poolInstance(store);
			return null;
		}
		store.loadingPage.pageIn();
		store.putLength=se.getLength();
		store.getLength=0;
		store.digest=se.getDigest();
		se.addStore(store);
		Log.debug(logger,"open.storeId:",store.storeId,":sid:",store.getStoreId(),":this:",store);
		return store;
	}
//	public static void remove(long storeId){
//		persistenceStore.remove(storeId);
//	}
	
	public long getStoreId(){
		return storeId;
	}
	
	public String getDigest(){
		return digest;
	}
	
	public Kind getKind() {
		return kind;
	}

	public long getPutLength() {
		return putLength;
	}
	
	public long getGetLength() {
		return getLength;
	}
	
	/**
	 * close���N�G�X�g���󂯎���Ă��邩�ۂ�
	 * 
	 */
	public boolean isCloseReceived(){
		return (isFin || isClosed || isCloseWait);
	}
	
	public MessageDigest getMessageDigest(){
		if(messageDigest==null){
			try {
				messageDigest=MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				logger.error("MessageDigest.getInstance(MD5) error.",e);
				throw new RuntimeException("MessageDigest.getInstance(MD5) error.",e);
			}
		}
		return messageDigest;
	}
	
	/**
	 * �����ɃC�x���g���������邩�ۂ�
	 * @return
	 */
	private boolean isEventSoon(){
		if(loadingPage!=null){
			//������onPageIn�C�x���g������
			return true;
		}else if(!savingPages.isEmpty()){
			//������onPageOut�C�x���g������
			return true;
		}else if(isOnAsyncBuffer && bufferGetter==null){
			//������callback�����
			return true;
		}
		return false;
	}
	
	//put���[�h��close���v�����ꂽ�ꍇ,callback����O�ɒ�������
	private void putFin(){
		deregister(storeId);
		if(isClosePersistance && getLength==putLength){
			long newStoreId=persistenceStore.add(storeId, topPageId, putLength,digest);
			if(storeId==newStoreId){
				return;//�ۑ�����page���c��
			}else{
				storeId=newStoreId;
			}
		}
		if(isClosePersistance && getLength!=putLength){//�ۑ������close�Ō���ꂽ�̂ɁA�S���o�͂�����Ȃ�����? 
			logger.error("sid:"+getStoreId() + " close error.getLength:"+getLength + " putLength:"+putLength);
		}
		//�ۑ��������K�v�Ȃ��Ȃ���Buffer�T�C�Y��o�^
		persistenceStore.addGarbageSize(pageOutSumLength);
	}
	
	private boolean checkFin(){
		if(isFin){//����fin�Ȃ牽�����Ȃ�
			return false;
		}
		if(!isCloseReceived()){
			return false;
		}
		if(isEventSoon()){//�����ɃC�x���g���������Ă������Ȃ牽�����Ȃ�
			return false;
		}
		//End��callback����O��storeId���Čv�Z����
		if(isPutCloseStoreId==false && kind==Kind.PUT){
			putFin();
			isPutCloseStoreId=true;
		}
		if(bufferGetter!=null){//�C�x���g���Ȃ��̂�callback�������o�^����Ă���
			Log.debug(logger,"checkFin asyncBufferEnd");
			if(isClosed){
				return false;
			}
			isClosed=true;//GET�ŏI�[�܂œǂ񂾏ꍇ�A�I����ʒm����
			//PUT��asyncClose�����ꍇ�͂�����ʂ�
			StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
			storeCallback.asyncBufferEnd(this,bufferGetter,userContext);
			bufferGetter=null;
			if(kind==Kind.PUTGET){
				closePutGetStore();//Page�ނ��N���A
			}
			return false;
		}
		if(kind==Kind.PUTGET){
			//�ۑ��������K�v�Ȃ��Ȃ���Buffer�T�C�Y��o�^
			deregister(storeId);
			persistenceStore.addGarbageSize(pageOutSumLength);
		}
		isFin=true;
		//Store�̎������s�����̂ŁA���v�����W�v����
		StoreManager.countStoreStastics(this);
		unref();
		return true;
	}
	
	//PUT���[�h��close��������
	private void closePutStore(){
		if(puttingPage!=null){
			if(isClosePersistance){
				savingPages.put(puttingPage.getPageId(),puttingPage);
				puttingPage.pageOut();
				puttingPage=null;
			}else{//��������Ǖۑ����Ȃ��ꍇ
				puttingPage.free(true);//�ۑ����Ȃ��Ȃ�pageFile����K�v�Ȃ�
				puttingPage=null;
			}
		}
	}
	
	//GET|PUTGET���[�h�ő�����close����ꍇ(close)�̏�������
	//������close���Ȃ��ꍇ(asyncClose)�́A���ɏ��������͂Ȃ�
	private void closePutGetStore(){
		//TODO �Ăяo����SoreCallback���Ȃ��AbufferGetter���o�^����Ă���Β��ڌĂяo��
		if(gettingPage!=null){
			if(gettingPage==puttingPage){
				puttingPage=null;
			}
			freeGettingPage();
		}
		if(puttingPage!=null){
			puttingPage.free(true);//�ۑ����Ȃ��Ȃ�pageFile����K�v�Ȃ�
			puttingPage=null;
		}
		if(bufferGetter!=null){
			if(isClosed==false){
				bufferGetter.onBufferEnd(userContext);//���ڌďo��
				isClosed=true;//onBufferEnd���Ăяo�����̂�close�ʒm
			}
			bufferGetter=null;
			isOnAsyncBuffer=false;
			isOnAsyncBufferRequest=false;
		}
	}
	
	/**
	 * GET���[�h��r����close�������ꍇ�Ɏg�p
	 */
	public synchronized void close(){
		close(false);
	}

	/**
	 * PUT���[�h�̏ꍇ�L��
	 * 
	 */
	public synchronized void close(boolean isPersistance){
		close(isPersistance,false);
	}
	
	/**
	 * PUT���[�h�̏ꍇ�L��
	 * 
	 */
	public synchronized void close(boolean isPersistance,boolean isAsync){
		BufferGetter bufferGetter=null;
		Object userContext=null;
		switch(kind){
		case GET:
			isAsync=false;
			break;
		case PUTGET:
			isAsync=false;
			break;
		case PUT:
			if(isPersistance){
				bufferGetter=dmmyGetter;
				userContext=this;
			}else{
				isAsync=false;
			}
			break;
		}
		if(isAsync){
			synchronized(this){
				close(bufferGetter,userContext);
				try {
					wait();
				} catch (InterruptedException e) {
				}
			}
		}else{
			close(bufferGetter,userContext);
		}
	}
	
	/**
	 * close������callback�Œʒm���ė~�����ꍇ�Ɏg�p
	 * 
	 */
	public synchronized void close(BufferGetter bufferGetter,Object userContext){
		if(isCloseReceived()){
			logger.error("duplicate close",new Exception());
			return;
		}
		switch(kind){
		case GET:
			if(bufferGetter!=null){
				asyncBuffer(bufferGetter, userContext);
			}
			if(isOnAsyncBuffer==false && this.bufferGetter!=null){
				closePutGetStore();
			}else{
				this.bufferGetter=bufferGetter;
				this.userContext=userContext;
				isOnAsyncBufferClose=true;
			}
			checkFin();
			return;
		case PUTGET:
			isCloseWait=true;
			isClosePersistance=true;
			if(bufferGetter==null){//TODO ����close�������ꍇ
				if(isOnAsyncBuffer==false && this.bufferGetter!=null){
					closePutGetStore();
				}else{
					isOnAsyncBufferClose=true;
				}
				checkFin();
				return;
			}
			break;
		case PUT:
			isCloseWait=true;
			isClosePersistance=true;
			if(bufferGetter==null){//�ۑ����Ȃ��w��
				isClosed=true;
				isClosePersistance=false;
				closePutStore();
				checkFin();
				return;
			}
			//����putBuffer���Ȃ��̂�digest���v�Z���Ă悢
			digest=DataUtil.digest(messageDigest);
			closePutStore();
			break;
		}
		if(bufferGetter==null){
			Log.debug(logger,"close use dmmyGetter");
			bufferGetter=dmmyGetter;
			userContext=this;
		}
		
		isOnAsyncBuffer=true;
		isOnAsyncBufferRequest=false;//2�d�Ăяo���͂Ȃ��������̂Ƃ���
		this.bufferGetter=bufferGetter;
		this.userContext=userContext;
		if(kind==Kind.PUTGET){
			callbackGetter();
		}
		checkFin();
	}
	
	/*
	Store��close�������AasyncBuffer���Ăяo����Ă���΁A������bufferEnd��callback����
	1)�����Ăяo����(StoreCallback�ɗ��񂶂����)�Ȃ�A���̊�����҂�
	--->���̌�̏�����asyncBuffer�Ăяo�������Ȃ�bfferEnd���Ă�
	--->���̌�̏�����asyncBuffer���������Ȃ牽�����Ȃ�
	2)�܂��Ăяo���Ă��Ȃ���΁A����callback��bufferEnd���Ă�
	*/
	
	/*
	 * close���͈ȉ��̓_�l������K�v������B
	 * 1)Store�͉����J�������̂��H
	 * 2)�A�v���ւ̒ʒm�͂ǂ̃^�C�~���O�Ŏ��{�����̂��H
	 * 3)PUT�̏ꍇ�AstoreId���擾����^�C�~���O���A�v���ɂ��邩�H
	 */
	
	private static class DmmyGetter implements BufferGetter{
		public boolean onBuffer(ByteBuffer[] buffers, Object userContext) {
			Log.debug(logger,"DmmyGetter onBuffer");
			return true;
		}
		public void onBufferEnd(Object userContext) {
			Log.debug(logger,"DmmyGetter onBufferEnd");
			synchronized(userContext){
				userContext.notify();
			}
		}
		public void onBufferFailure(Throwable failure, Object userContext) {
			Log.debug(logger,"DmmyGetter onBufferFailure.",failure);
			synchronized(userContext){
				userContext.notify();
			}
		}
	}
	
	//PUT�̏ꍇ�ABufferGetter����������̂��ʓ|�ȏꍇ������B
	private static DmmyGetter dmmyGetter=new DmmyGetter();
	
	public synchronized void putBuffer(ByteBuffer[] buffers){
		Log.debug(logger,"putBuffer.sid:",getStoreId());
		if(isCloseReceived()){
			logger.error("putBuffer aleady close");
			return;
		}
		if(puttingPage==null){
			throw new IllegalStateException("alrady closed");
		}
		if(kind==Kind.PUT){
			for(ByteBuffer buffer:buffers){
				ByteBuffer buf=buffer;
				int pos=buf.position();
				int len=buf.limit()-pos;
				messageDigest.update(buf.array(),pos,len);
			}
		}
		long thisPutLength=BuffersUtil.remaining(buffers);
		putBufferCount++;
		putBufferLength+=thisPutLength;
		
		putLength+=thisPutLength;
		if(puttingPage.putBuffer(buffers)){
			//puttingPage�ɋl�ߍ��߂��ꍇ
			if(kind==Kind.PUTGET){
				callbackGetter();
			}
			return;
		}
		//puttingPage�ɋl�ߍ��ݑ��Ȃ����ꍇ
		Page orgPuttingPage=puttingPage;
		puttingPage=Page.allocPage(this,orgPuttingPage);
		puttingPage.putBuffer(buffers);
		if(orgPuttingPage!=gettingPage){
//			logger.warn("putBuffer pageOut.",new Throwable());
			savingPages.put(orgPuttingPage.getPageId(),orgPuttingPage);
			orgPuttingPage.pageOut();
		}
		if(kind==Kind.PUTGET){
			callbackGetter();
		}
		return;
	}
	
	/**
	 * 
	 * @param bufferGetter
	 * @param userContext
	 * @return method�̒���callback�����ꍇ
	 */
	public synchronized boolean asyncBuffer(BufferGetter bufferGetter,Object userContext){
		if(isCloseReceived()){
			Log.debug(logger,"asyncBuffer aleady close");//�Ƃ��ǂ���������
			return false;
		}
		Log.debug(logger,"asyncBuffer.isOnAsyncBuffer:",isOnAsyncBuffer,":bufferGetter:",bufferGetter,":this:",this);
		if(isOnAsyncBuffer){
			if(this.bufferGetter==null){
				this.bufferGetter=bufferGetter;
				this.userContext=userContext;
				isOnAsyncBufferRequest=true;
			}else if(this.bufferGetter!=bufferGetter){
				logger.error("not same bufferGetter.",new Throwable());
			}
			Log.debug(logger,"callbackGetter return false isOnAsyncBuffer:true");
			return false;
		}
		isOnAsyncBuffer=true;
		this.bufferGetter=bufferGetter;
		this.userContext=userContext;
		if(callbackGetter()){
			Log.debug(logger,"callbackGetter return true");
			return true;
		}
		Log.debug(logger,"callbackGetter return false");
		return false;
	}
	
	//Page����Ăяo�����
	synchronized void onPageOut(naru.async.store.Page page) {
		Log.debug(logger,"onPageOut.page:",page,":sid:",getStoreId());
		long pageOutLength=page.getBufferLength();
		pageOutSumLength+=pageOutLength;
		if(page==loadingPage){//save����load���Ăяo���ꂽ
			//buffer�͊��Ɏ����Ă���̂ōēǂݍ��݂̕K�v������
//			Log.debug(logger,"//buffer�͊��Ɏ����Ă���̂ōēǂݍ��݂̕K�v������");
			StoreManager.preparePageIn(page);
			loadingPage.pageIn();
			return;
		}
		Page p=savingPages.remove(page.getPageId());
		if(p==null){
			logger.error("onPageOut error.page="+page);
		}
		
		//PUT���[�h�̏ꍇ�́APageOut��get�Ƃ݂Ȃ�
		if(kind==Kind.PUT){
			getLength+=pageOutLength;
		}
		page.free(false);
		checkFin();
	}
	
	/*
	 * gettingPage���J���A
	 * PUTGET�̏ꍇ�A����pageId���ė��p���Ă悢���A
	 * GET�̏ꍇ�́Apersistence�f�[�^�������Ă���̂ōė��p�ł��Ȃ�
	 */
	private synchronized void freeGettingPage(){
		if(kind==Kind.GET){
			gettingPage.free(false);
		}else{//PUTGET
			gettingPage.free(true);
		}
		gettingPage=null;
	}
	
	synchronized void onPageIn(naru.async.store.Page page) {
		Log.debug(logger,"onPageIn.page:",page);
		if(page.getStoreId()!=storeId){
			logger.error("onPageIn Error.page.getStoreId():"+page.getStoreId()+" storeId:"+storeId,new Exception());
		}
		StoreManager.donePageIn(page);
		
		if(gettingPage!=null){
			logger.error("onPageIn error.gettingPage="+gettingPage);
			throw new RuntimeException("onPageIn error.gettingPage="+gettingPage);
		}
		if(page!=loadingPage){
			logger.error("onPageIn error.page!=loadingPage");
			throw new RuntimeException("onPageIn error.page!=loadingPage");
		}
		gettingPage=loadingPage;
		loadingPage=null;
		if(isClosed){
			freeGettingPage();
		}else{
			callbackGetter();
		}
		checkFin();
	}
	
	/**
	 * �r���̒�����Ăяo�����
	 * 
	 * @param userContext
	 * @param bufferGetter
	 * @return callback�����ꍇtrue
	 */
	private boolean callbackGetter(){
		if(bufferGetter==null){
			return false;
		}
		if(failure!=null){//error���������Ă����failure��callback
			if(isClosed){
				return true;
			}
			isClosed=true;//GET�ŏI�[�܂œǂ񂾏ꍇ�A�I����ʒm����
			StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
			storeCallback.asyncBufferFailure(this, bufferGetter, userContext, failure);
			bufferGetter=null;
			closePutGetStore();//Page�ނ��N���A
			return true;
		}
		ByteBuffer[] buffer=null;
		long nextPageId=FREE_ID;
		if(loadingPage==null && gettingPage==null){//�I�[�ɒB���Ă���
			if(isClosed){
				return true;
			}
			//�񓯊� onBufferEnd�Ăяo��
			isClosed=true;//GET�ŏI�[�܂œǂ񂾏ꍇ�A�I����ʒm����
			StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
			storeCallback.asyncBufferEnd(this,bufferGetter,userContext);
			bufferGetter=null;
			closePutGetStore();//Page�ނ��N���A
			return true;
		}else if(gettingPage!=null){
			buffer=gettingPage.getBuffer();
			nextPageId=gettingPage.getNextPageId();
			//puttingPage��gettingPage���אڂ��Ă���ꍇ�A��������buffer���Ƃ��ăR���J�`����
			if(puttingPage!=null && puttingPage.getPageId()==nextPageId){
				ByteBuffer[] nextBuffer=puttingPage.getBuffer();
				freeGettingPage();
				gettingPage=puttingPage;
				buffer=BuffersUtil.concatenate(buffer, nextBuffer);
//				Log.debug(logger,"//puttingPage��gettingPage���אڂ��Ă���ꍇ�A��������buffer���Ƃ��ăR���J�`");
			}
		}
		if(buffer==null){
//			Log.debug(logger,"//buffer ��null�ł���");
			return false;
		}
		getLength+=BuffersUtil.remaining(buffer);
		//gettingPage��puttingPage�������ꍇ�́APUTGET�œ����PAGE�����Ă�����
		if(gettingPage!=puttingPage){
//			Log.debug(logger,"//gettingPage!=puttingPage:",getLength);
			freeGettingPage();
			if(puttingPage!=null && puttingPage.getPageId()==nextPageId){
				//���y�[�W���AputtingPage�̏ꍇ,��Ŋ��Ƀ`�F�b�N�ς݁H����Ȃ��C������
				gettingPage=puttingPage;
			}else if(nextPageId>=0){//���y�[�W������ꍇ
				//���y�[�W�́ApageOut����Ă���ꍇ
				Page savingPage=savingPages.remove(nextPageId);
				if(savingPage!=null){//���y�[�W��save����page������
					loadingPage=savingPage;//onPageOut��load����
				}else{
					loadingPage=StoreManager.preparePageIn(this,nextPageId);
					loadingPage.pageIn();
				}
			}
		}
		//�񓯊� onBuffer�Ăяo��
		StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
//		StoreCallback storeCallback=new StoreCallback();
		storeCallback.asyncBuffer(this,bufferGetter,userContext,buffer);
		bufferGetter=null;
		return true;
	}
	
	//isEnd�͎g�p���Ă��Ȃ�
	public synchronized void doneCallback(boolean isEnd,boolean onBufferReturn,BufferGetter orgBuferGetter,Object orgUserContext) {
		Log.debug(logger,"doneCallback:",this,":onBufferReturn:",onBufferReturn,":isOnAsyncBufferRequest:",isOnAsyncBufferRequest);
		if(isOnAsyncBufferClose){//�Ăяo������close�v����������
			isOnAsyncBufferRequest=false;
			isOnAsyncBufferClose=false;
			if(onBufferReturn){
				bufferGetter=orgBuferGetter;
				userContext=orgUserContext;
			}else if(bufferGetter==null){
				isOnAsyncBuffer=false;
			}
			closePutGetStore();
			callbackGetter();//bufferGetter,userContext�́A�ݒ�ς�
		}else if(isOnAsyncBufferRequest){//�Ăяo�����ɍ�calback�v����������
			isOnAsyncBufferRequest=false;
			callbackGetter();//bufferGetter,userContext�́A�ݒ�ς�
		}else if(onBufferReturn){//onBuffer��true�ŕ��A����
			isOnAsyncBufferRequest=false;
			bufferGetter=orgBuferGetter;
			userContext=orgUserContext;
			callbackGetter();
		}else if(isEnd==false&&isCloseReceived()){//�I�����󂯎�������AonBufferEnd��onBufferFailure���ʒm��
			isOnAsyncBufferRequest=false;
			bufferGetter=orgBuferGetter;
			userContext=orgUserContext;
			callbackGetter();
		}else{
			bufferGetter=null;
			userContext=null;
			isOnAsyncBuffer=false;
			isOnAsyncBufferRequest=false;
		}
		checkFin();
	}
	
	/**
	 * �������ɃG���[�����������ꍇ�ɌĂ΂��
	 * @param failure
	 */
	synchronized void onFailure(Throwable failure) {
		this.failure=failure;
		callbackGetter();
	}
	
	/**
	 * callback�Ǘ����\�b�h�S
	 */
	//queue���ꂽ���Ԃ�callback����
	//callback���͏d�˂�callback���Ȃ�
	private LinkedList<StoreCallback> callbackQueue=new LinkedList<StoreCallback>();
	private boolean isCallbackProcessing=false;
	void callbackQueue(StoreCallback storeCallback){
		Log.debug(logger,"callbackQueue sid:",getStoreId());
		synchronized(callbackQueue){
			callbackQueue.addLast(storeCallback);
		}
		StoreManager.asyncDispatch(this);//TODO
	}
	
	void callback(){
		Log.debug(logger,"callback sid:",getStoreId());
		StoreCallback storeCallback=null;
		synchronized(callbackQueue){
			if(isCallbackProcessing || callbackQueue.size()<=0){
				Log.debug(logger,"callback loopout sid:",getPoolId());
				return;
			}
			isCallbackProcessing=true;
			storeCallback=callbackQueue.removeFirst();
		}
		while(true){
			storeCallback.callback();
			storeCallback.unref(true);
			synchronized(callbackQueue){
				if(callbackQueue.size()<=0){
					isCallbackProcessing=false;
					break;
				}
				storeCallback=callbackQueue.removeFirst();
			}
		}
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
	public int getPutBufferCount() {
		return putBufferCount;
	}
	public long getPutBufferLength() {
		return putBufferLength;
	}
	public int getOnBufferCount() {
		return onBufferCount;
	}
	public long getOnBufferLength() {
		return onBufferLength;
	}
	public int getOnBufferEndCount() {
		return onBufferEndCount;
	}
	public int getOnBufferFailureCount() {
		return onBufferFailureCount;
	}
}
