package naru.async.cache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.collections.map.MultiKeyMap;
import org.apache.log4j.Logger;

import naru.async.Timer;
import naru.async.store.Page;
import naru.async.store.StoreManager;
import naru.async.timer.TimerManager;

public class BufferCache implements Timer{
	private static Logger logger=Logger.getLogger(BufferCache.class);
	private static final long INTERVAL=10000;
	private static BufferCache instance=new BufferCache();
	public static BufferCache getInstance(){
		return instance;
	}
	private BufferCache(){
		timer=TimerManager.setInterval(INTERVAL, this, null);
	}
	private Object timer;
	private Map<Long,BufferInfo> pageCache=new HashMap<Long,BufferInfo>();
	private Map<Long,BufferInfo> tmpPageCache=new HashMap<Long,BufferInfo>();
	private MultiKeyMap filePositionCache=new MultiKeyMap();
	private MultiKeyMap tmpFilePositionCache=new MultiKeyMap();
	private int min=2048;//16k bufferの場合約32Mキャッシュを持つ
	private int max=min*2;
	private int overFlow=0;
	private int hit=0;
	private int miss=0;
	
	public void setCacheSize(int min){
		this.min=min;
		this.max=min*2;
	}
	
	private boolean check(BufferInfo bufferInfo,long now){
		if(bufferInfo.isChange()){//変更があるか?
			return false;
		}
		float lastScore=bufferInfo.getLastScore();
		float score=bufferInfo.getScore(now);
		if(lastScore>scoreThreshold&&score>lastScore){
			return false;
		}
		//TODO 全部入ったとしても、minを超えない場合は、addしないようにする
		scores.add(score);
		return true;
	}
	
	public void term(){
		TimerManager.clearInterval(timer);
		logger.info("BufferCache term: pageCache.size:"+pageCache.size()+":filePositionCache.size:"+filePositionCache.size()+":min:"+min+":hit:"+hit+":miss:"+miss+":overFlow:"+overFlow);
		
		synchronized(pageCache){
			Iterator<BufferInfo> itr=pageCache.values().iterator();
			while(itr.hasNext()){
				BufferInfo fileInfo=itr.next();
				itr.remove();
				fileInfo.unref();
			}
			Iterator<BufferInfo> tmpItr=tmpPageCache.values().iterator();
			while(tmpItr.hasNext()){
				BufferInfo fileInfo=itr.next();
				itr.remove();
				fileInfo.unref();
			}
			max=0;
		}
		
		synchronized(filePositionCache){
			Iterator<BufferInfo> itr=filePositionCache.values().iterator();
			while(itr.hasNext()){
				BufferInfo fileInfo=itr.next();
				itr.remove();
				fileInfo.unref();
			}
			Iterator<BufferInfo> tmpItr=tmpFilePositionCache.values().iterator();
			while(tmpItr.hasNext()){
				BufferInfo fileInfo=itr.next();
				itr.remove();
				fileInfo.unref();
			}
		}
	}
		
	public ByteBuffer[] get(Page page){
		BufferInfo cacheInfo=pageCache.get(page.getPageId());
		if(cacheInfo==null){
			miss++;
			return null;
		}
		hit++;
//		cacheInfo.ref();
		return cacheInfo.duplicateBuffers();
	}
	
	public ByteBuffer[] get(FileInfo fileInfo,long filePosition){
		BufferInfo cacheInfo=(BufferInfo)filePositionCache.get(fileInfo,filePosition);
		if(cacheInfo==null){
			miss++;
			return null;
		}
		hit++;
//		cacheInfo.ref();
		return cacheInfo.duplicateBuffers();
	}
	
	public void put(Page page,ByteBuffer[] buffer){
		if((filePositionCache.size()+pageCache.size())>=max){
			overFlow++;
			return;
		}
		long pageId=page.getPageId();
		long storeId=page.getStoreId();
		long totalLength=StoreManager.getStoreLength(storeId);
		if(totalLength<0){
			return;//無効なstoreId
		}
		BufferInfo bufferInfo=pageCache.get(pageId);
		if(bufferInfo!=null){
			return;//登録済み
		}
		bufferInfo=BufferInfo.create(buffer,totalLength,storeId);
		BufferInfo orgInfo=null;
		synchronized(pageCache){
			if(isPageCheck){
				orgInfo=tmpPageCache.put(pageId, bufferInfo);
			}else{
				orgInfo=pageCache.put(pageId, bufferInfo);
			}
		}
		if(orgInfo!=null){//チェック直後なので殆どないはず
			orgInfo.unref();
		}
	}
	
	public void put(FileInfo fileInfo,long filePosition,ByteBuffer[] buffer){
		if((filePositionCache.size()+pageCache.size())>=max){
			overFlow++;
			return;
		}
		BufferInfo cacheInfo=(BufferInfo)filePositionCache.get(fileInfo,filePosition);
		if(cacheInfo!=null){
			return;//登録済み
		}
		cacheInfo=BufferInfo.create(buffer,fileInfo.length(),fileInfo);
		BufferInfo orgInfo=null;
		synchronized(filePositionCache){
			if(isFileCheck){
				orgInfo=(BufferInfo)tmpFilePositionCache.put(fileInfo,filePosition, cacheInfo);
			}else{
				orgInfo=(BufferInfo)filePositionCache.put(fileInfo,filePosition, cacheInfo);
			}
		}
		if(orgInfo!=null){//チェック直後なので殆どないはず
			orgInfo.unref();
		}
	}

	private boolean isPageCheck=false;
	private boolean isFileCheck=false;
	private float scoreThreshold=Float.MAX_VALUE;
	private ArrayList<Float> scores=new ArrayList<Float>();
	
	private void checkPageCache(){
		synchronized(pageCache){
			isPageCheck=true;
		}
		long now=System.currentTimeMillis();
		Iterator<BufferInfo> itr=pageCache.values().iterator();
		while(itr.hasNext()){
			BufferInfo bufferInfo=itr.next();
			if(!check(bufferInfo,now)){
				itr.remove();
				bufferInfo.unref();
				continue;
			}
		}
		synchronized(pageCache){
			isPageCheck=false;
			pageCache.putAll(tmpPageCache);//重複した場合、BufferInfoが漏れる可能性あり
			tmpPageCache.clear();
		}
	}
	
	private void checkFileCache(){
		synchronized(filePositionCache){
			isFileCheck=true;
		}
		long now=System.currentTimeMillis();
		Iterator<BufferInfo> itr=filePositionCache.values().iterator();
		while(itr.hasNext()){
			BufferInfo bufferInfo=itr.next();
			if(!check(bufferInfo,now)){
				itr.remove();
				bufferInfo.unref();
				continue;
			}
		}
		synchronized(filePositionCache){
			isFileCheck=false;
			filePositionCache.putAll(tmpFilePositionCache);//重複した場合、BufferInfoが漏れる可能性あり
			tmpFilePositionCache.clear();
		}
	}
	
	public void onTimer(Object userContext) {
		checkPageCache();
		checkFileCache();
		if(scores.size()<min){
			scoreThreshold=Float.MAX_VALUE;
		}else{
			Collections.sort(scores);
			scoreThreshold=scores.get(min);
		}
		scores.clear();
	}
}
