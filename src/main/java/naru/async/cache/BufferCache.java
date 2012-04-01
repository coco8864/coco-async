package naru.async.cache;

import java.nio.ByteBuffer;
import java.util.HashMap;
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
	private Map<FileInfo,Map<Long,BufferInfo>> filePositionCache=new HashMap<FileInfo,Map<Long,BufferInfo>>();
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
			return;//�o�^�ς�
		}
		bufferInfo=BufferInfo.create(buffer,totalLength);
		BufferInfo orgInfo=null;
		synchronized(pageCache){
			orgInfo=pageCache.put(pageId, bufferInfo);
		}
		if(orgInfo!=null){//�`�F�b�N����Ȃ̂Ŗw�ǂȂ��͂�
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
			return;//�o�^�ς�
		}
		cacheInfo=BufferInfo.create(buffer,fileInfo.getLength());
		BufferInfo orgInfo=null;
		synchronized(positionCache){
			filePositionCurrent++;
			orgInfo=positionCache.put(filePosition, cacheInfo);
		}
		if(orgInfo!=null){//�`�F�b�N����Ȃ̂Ŗw�ǂȂ��͂�
			orgInfo.unref();
		}
	}

	public void onTimer(Object userContext) {
	}
	
}
