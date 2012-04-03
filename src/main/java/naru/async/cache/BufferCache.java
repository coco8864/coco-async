package naru.async.cache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import naru.async.Timer;
import naru.async.store.Page;
import naru.async.timer.TimerManager;

public class BufferCache implements Timer{
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
	private Map<FileInfo,Map<Long,BufferInfo>> filePositionCache=new HashMap<FileInfo,Map<Long,BufferInfo>>();
	private Map<FileInfo,Map<Long,BufferInfo>> tmpFilePositionCache=new HashMap<FileInfo,Map<Long,BufferInfo>>();
	private int filePositionCurrent=0;
	private int min=128;
	private int max=512;
	private int overFlow=0;
	private int hit=0;
	private int miss=0;
		
	public ByteBuffer[] get(Page page){
		BufferInfo cacheInfo=pageCache.get(page.getPageId());
		if(cacheInfo==null){
			miss++;
			return null;
		}
		hit++;
		cacheInfo.ref();
		return cacheInfo.duplicateBuffers();
	}
	
	public ByteBuffer[] get(FileInfo fileInfo,long filePosition){
		Map<Long,BufferInfo> positionCache=filePositionCache.get(fileInfo);
		if(positionCache==null){
			miss++;
			return null;
		}
		BufferInfo cacheInfo=positionCache.get(filePosition);
		if(cacheInfo==null){
			miss++;
			return null;
		}
		hit++;
		cacheInfo.ref();
		return cacheInfo.duplicateBuffers();
	}
	
	public void put(Page page,ByteBuffer[] buffer,long totalLength){
		if((filePositionCurrent+pageCache.size())>=max){
			overFlow++;
			return;
		}
		long pageId=page.getPageId();
		BufferInfo bufferInfo=pageCache.get(pageId);
		if(bufferInfo!=null){
			return;//ìoò^çœÇ›
		}
		bufferInfo=BufferInfo.create(buffer,totalLength);
		BufferInfo orgInfo=null;
		synchronized(pageCache){
			if(isPageCheck){
				orgInfo=tmpPageCache.put(pageId, bufferInfo);
			}else{
				orgInfo=pageCache.put(pageId, bufferInfo);
			}
		}
		if(orgInfo!=null){//É`ÉFÉbÉNíºå„Ç»ÇÃÇ≈ñwÇ«Ç»Ç¢ÇÕÇ∏
			orgInfo.unref();
		}
	}
	
	public void put(FileInfo fileInfo,long filePosition,ByteBuffer[] buffer){
		if((filePositionCurrent+pageCache.size())>=max){
			overFlow++;
			return;
		}
		Map<Long,BufferInfo> positionCache=filePositionCache.get(fileInfo);
		if(positionCache==null){
			positionCache=new HashMap<Long,BufferInfo>();
			filePositionCache.put(fileInfo,positionCache);
		}
		BufferInfo cacheInfo=positionCache.get(filePosition);
		if(cacheInfo!=null){
			return;//ìoò^çœÇ›
		}
		cacheInfo=BufferInfo.create(buffer,fileInfo.getLength());
		BufferInfo orgInfo=null;
		synchronized(positionCache){
			if(isFileCheck){
				
			}else{
				orgInfo=positionCache.put(filePosition, cacheInfo);
			}
			if(orgInfo==null){
				filePositionCurrent++;
			}
		}
		if(orgInfo!=null){//É`ÉFÉbÉNíºå„Ç»ÇÃÇ≈ñwÇ«Ç»Ç¢ÇÕÇ∏
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
			if(check(bufferInfo,now)){
				itr.remove();
				bufferInfo.unref();
				continue;
			}
		}
		synchronized(pageCache){
			isPageCheck=false;
			pageCache.putAll(tmpPageCache);
			tmpPageCache.clear();
		}
	}
	
	private void checkFileCache(){
		synchronized(filePositionCache){
			isFileCheck=true;
		}
		long now=System.currentTimeMillis();
		Iterator<Map<Long,BufferInfo>> positionItr=filePositionCache.values().iterator();
		while(positionItr.hasNext()){
			Map<Long,BufferInfo> positionCache=positionItr.next();
			Iterator<BufferInfo> itr=positionCache.values().iterator();
			int positionCount=0;
			while(itr.hasNext()){
				BufferInfo bufferInfo=itr.next();
				if(check(bufferInfo,now)){
					itr.remove();
					bufferInfo.unref();
					continue;
				}
				positionCount++;
			}
			if(positionCount==0){
				positionItr.remove();
			}
		}
		synchronized(filePositionCache){
			isFileCheck=false;
			filePositionCache.putAll(tmpFilePositionCache);
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
