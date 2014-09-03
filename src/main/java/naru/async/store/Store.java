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
	//persistenceStore群を管理する(同一stoerIdに対して複数openする場合がある)
	private static PersistenceStore persistenceStore;
	public static final long FREE_ID=-1;
	
	public static void init(PersistenceStore persistenceStore){
		Store.persistenceStore=persistenceStore;
	}
	public static void term(){
	}
	
	//PUTで未close,PUTGETのStore群を管理する(stoerIdで一意に識別できる)
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
	private Page gettingPage;//get対象Page
	private Page puttingPage;//put対象Page
	private Map<Long,Page> savingPages=new HashMap<Long,Page>();//save中Page
	private Page loadingPage;//load中Page
	private Throwable failure;//処置中に発生したerror
	
	private Kind kind;
	private long storeId=-1;
	private long topPageId;//先頭PageのpageId,PUT,GETの場合有効、PUTGETの場合-1
	private String digest;
	private long pageOutSumLength;//pageOutした総計サイズ,compress判定に使用
	private long putLength;
	private long getLength;
	private boolean isFin=false;//実closeを呼んだ(unref済み）
	private boolean isCloseWait=false;//asyncCloseを受け付けた
	private boolean isClosed=false;//closeを受け付けた
	private boolean isClosePersistance=false;//close受付時に指定されたオプション
	private boolean isPutCloseStoreId=false;//put　close後 storeIdを再計算したか否か
	
	private boolean isOnAsyncBuffer;//callback処理中
	private boolean isOnAsyncBufferRequest;//callback処理中にasyncReadあり
	private boolean isOnAsyncBufferClose;//callback処理中にcloseあり
	private BufferGetter bufferGetter;//asyncBufferあり
	private Object userContext;
	
	private MessageDigest messageDigest;//=MessageDigest.getInstance("MD5");
//	private StoreStastices stastics=new StoreStastices();
	
	//統計情報
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
		if(isPersistence){//保存専用
			store.getMessageDigest();//messageDigestの準備
			store.kind=Kind.PUT;
			store.gettingPage=null;
		}else{//読み書きバッファ
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
	 * closeリクエストを受け取っているか否か
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
	 * すぐにイベントが発生するか否か
	 * @return
	 */
	private boolean isEventSoon(){
		if(loadingPage!=null){
			//すぐにonPageInイベントが来る
			return true;
		}else if(!savingPages.isEmpty()){
			//すぐにonPageOutイベントが来る
			return true;
		}else if(isOnAsyncBuffer && bufferGetter==null){
			//すぐにcallbackされる
			return true;
		}
		return false;
	}
	
	//putモードでcloseが要求された場合,callbackする前に調整する
	private void putFin(){
		deregister(storeId);
		if(isClosePersistance && getLength==putLength){
			long newStoreId=persistenceStore.add(storeId, topPageId, putLength,digest);
			if(storeId==newStoreId){
				return;//保存したpageを残す
			}else{
				storeId=newStoreId;
			}
		}
		if(isClosePersistance && getLength!=putLength){//保存しろとcloseで言われたのに、全部出力しきらなかった? 
			logger.error("sid:"+getStoreId() + " close error.getLength:"+getLength + " putLength:"+putLength);
		}
		//保存したが必要なくなったBufferサイズを登録
		persistenceStore.addGarbageSize(pageOutSumLength);
	}
	
	private boolean checkFin(){
		if(isFin){//既にfinなら何もしない
			return false;
		}
		if(!isCloseReceived()){
			return false;
		}
		if(isEventSoon()){//すぐにイベントが挙がってきそうなら何もしない
			return false;
		}
		//Endをcallbackする前にstoreIdを再計算する
		if(isPutCloseStoreId==false && kind==Kind.PUT){
			putFin();
			isPutCloseStoreId=true;
		}
		if(bufferGetter!=null){//イベントがないのにcallbackだけが登録されている
			Log.debug(logger,"checkFin asyncBufferEnd");
			if(isClosed){
				return false;
			}
			isClosed=true;//GETで終端まで読んだ場合、終了を通知する
			//PUTでasyncCloseした場合はここを通る
			StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
			storeCallback.asyncBufferEnd(this,bufferGetter,userContext);
			bufferGetter=null;
			if(kind==Kind.PUTGET){
				closePutGetStore();//Page類をクリア
			}
			return false;
		}
		if(kind==Kind.PUTGET){
			//保存したが必要なくなったBufferサイズを登録
			deregister(storeId);
			persistenceStore.addGarbageSize(pageOutSumLength);
		}
		isFin=true;
		//Storeの寿命が尽きたので、統計情報を集計する
		StoreManager.countStoreStastics(this);
		unref();
		return true;
	}
	
	//PUTモードのclose初期処理
	private void closePutStore(){
		if(puttingPage!=null){
			if(isClosePersistance){
				savingPages.put(puttingPage.getPageId(),puttingPage);
				puttingPage.pageOut();
				puttingPage=null;
			}else{//作ったけど保存しない場合
				puttingPage.free(true);//保存しないならpageFile上も必要ない
				puttingPage=null;
			}
		}
	}
	
	//GET|PUTGETモードで即座にcloseする場合(close)の初期処理
	//即座にcloseしない場合(asyncClose)は、特に初期処理はない
	private void closePutGetStore(){
		//TODO 呼び出したSoreCallbackがなく、bufferGetterが登録されていれば直接呼び出す
		if(gettingPage!=null){
			if(gettingPage==puttingPage){
				puttingPage=null;
			}
			freeGettingPage();
		}
		if(puttingPage!=null){
			puttingPage.free(true);//保存しないならpageFile上も必要ない
			puttingPage=null;
		}
		if(bufferGetter!=null){
			if(isClosed==false){
				bufferGetter.onBufferEnd(userContext);//直接呼出し
				isClosed=true;//onBufferEndを呼び出したのでclose通知
			}
			bufferGetter=null;
			isOnAsyncBuffer=false;
			isOnAsyncBufferRequest=false;
		}
	}
	
	/**
	 * GETモードを途中でcloseしたい場合に使用
	 */
	public synchronized void close(){
		close(false);
	}

	/**
	 * PUTモードの場合有効
	 * 
	 */
	public synchronized void close(boolean isPersistance){
		close(isPersistance,false);
	}
	
	/**
	 * PUTモードの場合有効
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
	 * close完了をcallbackで通知して欲しい場合に使用
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
			if(bufferGetter==null){//TODO 即刻closeしたい場合
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
			if(bufferGetter==null){//保存しない指定
				isClosed=true;
				isClosePersistance=false;
				closePutStore();
				checkFin();
				return;
			}
			//もうputBufferしないのでdigestを計算してよい
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
		isOnAsyncBufferRequest=false;//2重呼び出しはなかったものとする
		this.bufferGetter=bufferGetter;
		this.userContext=userContext;
		if(kind==Kind.PUTGET){
			callbackGetter();
		}
		checkFin();
	}
	
	/*
	Storeをcloseした時、asyncBufferが呼び出されていれば、即座にbufferEndをcallbackする
	1)もう呼び出し中(StoreCallbackに頼んじゃった)なら、その完了を待つ
	--->その後の処理でasyncBuffer呼び出し相当ならbfferEndを呼ぶ
	--->その後の処理でasyncBuffer解除相当なら何もしない
	2)まだ呼び出していなければ、そのcallbackでbufferEndを呼ぶ
	*/
	
	/*
	 * close問題は以下の点考慮する必要がある。
	 * 1)Storeは何時開放されるのか？
	 * 2)アプリへの通知はどのタイミングで実施されるのか？
	 * 3)PUTの場合、storeIdを取得するタイミングがアプリにあるか？
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
	
	//PUTの場合、BufferGetterを実装するのが面倒な場合がある。
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
			//puttingPageに詰め込めた場合
			if(kind==Kind.PUTGET){
				callbackGetter();
			}
			return;
		}
		//puttingPageに詰め込み損なった場合
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
	 * @return methodの中でcallbackした場合
	 */
	public synchronized boolean asyncBuffer(BufferGetter bufferGetter,Object userContext){
		if(isCloseReceived()){
			Log.debug(logger,"asyncBuffer aleady close");//ときどき発生する
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
	
	//Pageから呼び出される
	synchronized void onPageOut(naru.async.store.Page page) {
		Log.debug(logger,"onPageOut.page:",page,":sid:",getStoreId());
		long pageOutLength=page.getBufferLength();
		pageOutSumLength+=pageOutLength;
		if(page==loadingPage){//save中にloadが呼び出された
			//bufferは既に失われているので再読み込みの必要がある
//			Log.debug(logger,"//bufferは既に失われているので再読み込みの必要がある");
			StoreManager.preparePageIn(page);
			loadingPage.pageIn();
			return;
		}
		Page p=savingPages.remove(page.getPageId());
		if(p==null){
			logger.error("onPageOut error.page="+page);
		}
		
		//PUTモードの場合は、PageOutをgetとみなす
		if(kind==Kind.PUT){
			getLength+=pageOutLength;
		}
		page.free(false);
		checkFin();
	}
	
	/*
	 * gettingPageを開放、
	 * PUTGETの場合、このpageIdを再利用してよいが、
	 * GETの場合は、persistenceデータが入っているので再利用できない
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
	 * 排他の中から呼び出される
	 * 
	 * @param userContext
	 * @param bufferGetter
	 * @return callbackした場合true
	 */
	private boolean callbackGetter(){
		if(bufferGetter==null){
			return false;
		}
		if(failure!=null){//errorが発生していればfailureをcallback
			if(isClosed){
				return true;
			}
			isClosed=true;//GETで終端まで読んだ場合、終了を通知する
			StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
			storeCallback.asyncBufferFailure(this, bufferGetter, userContext, failure);
			bufferGetter=null;
			closePutGetStore();//Page類をクリア
			return true;
		}
		ByteBuffer[] buffer=null;
		long nextPageId=FREE_ID;
		if(loadingPage==null && gettingPage==null){//終端に達している
			if(isClosed){
				return true;
			}
			//非同期 onBufferEnd呼び出し
			isClosed=true;//GETで終端まで読んだ場合、終了を通知する
			StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
			storeCallback.asyncBufferEnd(this,bufferGetter,userContext);
			bufferGetter=null;
			closePutGetStore();//Page類をクリア
			return true;
		}else if(gettingPage!=null){
			buffer=gettingPage.getBuffer();
			nextPageId=gettingPage.getNextPageId();
			//puttingPageとgettingPageが隣接している場合、両方からbufferをとってコンカチする
			if(puttingPage!=null && puttingPage.getPageId()==nextPageId){
				ByteBuffer[] nextBuffer=puttingPage.getBuffer();
				freeGettingPage();
				gettingPage=puttingPage;
				buffer=BuffersUtil.concatenate(buffer, nextBuffer);
//				Log.debug(logger,"//puttingPageとgettingPageが隣接している場合、両方からbufferをとってコンカチ");
			}
		}
		if(buffer==null){
//			Log.debug(logger,"//buffer がnullでした");
			return false;
		}
		getLength+=BuffersUtil.remaining(buffer);
		//gettingPageとputtingPageが同じ場合は、PUTGETで同一のPAGEを見ている状態
		if(gettingPage!=puttingPage){
//			Log.debug(logger,"//gettingPage!=puttingPage:",getLength);
			freeGettingPage();
			if(puttingPage!=null && puttingPage.getPageId()==nextPageId){
				//次ページが、puttingPageの場合,上で既にチェック済み？いらない気がする
				gettingPage=puttingPage;
			}else if(nextPageId>=0){//次ページがある場合
				//次ページは、pageOutされている場合
				Page savingPage=savingPages.remove(nextPageId);
				if(savingPage!=null){//次ページはsave中のpageだった
					loadingPage=savingPage;//onPageOutでload処理
				}else{
					loadingPage=StoreManager.preparePageIn(this,nextPageId);
					loadingPage.pageIn();
				}
			}
		}
		//非同期 onBuffer呼び出し
		StoreCallback storeCallback=(StoreCallback)PoolManager.getInstance(StoreCallback.class);
//		StoreCallback storeCallback=new StoreCallback();
		storeCallback.asyncBuffer(this,bufferGetter,userContext,buffer);
		bufferGetter=null;
		return true;
	}
	
	//isEndは使用していない
	public synchronized void doneCallback(boolean isEnd,boolean onBufferReturn,BufferGetter orgBuferGetter,Object orgUserContext) {
		Log.debug(logger,"doneCallback:",this,":onBufferReturn:",onBufferReturn,":isOnAsyncBufferRequest:",isOnAsyncBufferRequest);
		if(isOnAsyncBufferClose){//呼び出し中にclose要求があった
			isOnAsyncBufferRequest=false;
			isOnAsyncBufferClose=false;
			if(onBufferReturn){
				bufferGetter=orgBuferGetter;
				userContext=orgUserContext;
			}else if(bufferGetter==null){
				isOnAsyncBuffer=false;
			}
			closePutGetStore();
			callbackGetter();//bufferGetter,userContextは、設定済み
		}else if(isOnAsyncBufferRequest){//呼び出し中に再calback要求があった
			isOnAsyncBufferRequest=false;
			callbackGetter();//bufferGetter,userContextは、設定済み
		}else if(onBufferReturn){//onBufferがtrueで復帰した
			isOnAsyncBufferRequest=false;
			bufferGetter=orgBuferGetter;
			userContext=orgUserContext;
			callbackGetter();
		}else if(isEnd==false&&isCloseReceived()){//終了を受け取ったが、onBufferEndもonBufferFailureも通知未
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
	 * 処理中にエラーが発生した場合に呼ばれる
	 * @param failure
	 */
	synchronized void onFailure(Throwable failure) {
		this.failure=failure;
		callbackGetter();
	}
	
	/**
	 * callback管理メソッド郡
	 */
	//queueされた順番にcallbackする
	//callback中は重ねてcallbackしない
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
	
	//統計関数,StoreCallbackから加算される、排他中に呼び出される
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
