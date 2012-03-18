package naru.async.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import naru.async.Timer;
import naru.async.pool.PoolManager;
import naru.async.timer.TimerManager;

public class PageCache implements Timer{
	private static PageCache instance;
	
	public static PageCache getInstance(){
		if(instance==null){
			instance=new PageCache();
		}
		return instance;
	}
	
	/* bufferをpoolで眠らせるより有効なデータを保持しているbufferは積極的に再利用するポリシー */
	//default　Buffer poolの状況をみてcacheからの追い出しを行う
	private static class CacheInfo{
		private boolean isPersiste;//Storeに保存してあるか否か
		private Page page;
		private ByteBuffer[] buffer;
		/* 統計情報 */
		private long inTime;
		private long lastGet;
		private int totalCount;
		private int count;
		private long totalLength;
	}

	private Map<Long,CacheInfo> cache=new HashMap<Long,CacheInfo>();
	private Map<Long,List<CacheInfo>> storeCache=new HashMap<Long,List<CacheInfo>>();
	private Object timer;
	
	private PageCache(){
		timer=TimerManager.setInterval(10000, this, null);
	}
	
	private boolean addInfo(CacheInfo info){
		long pageId=info.page.getPageId();
		long storeId=info.page.getStoreId();
		List<CacheInfo> storeInfos=storeCache.get(storeId);
		if(storeInfos==null){
			storeInfos=new ArrayList<CacheInfo>();
			storeCache.put(storeId, storeInfos);
		}else if(storeInfos.size()>=8){
			return false;
		}
		storeInfos.add(info);
		CacheInfo cacheInfo=cache.get(pageId);
		if(cacheInfo!=null){
			return false;
		}
		cache.put(pageId, info);
		return true;
	}
	
	private void removeInfo(CacheInfo info){
		long pageId=info.page.getPageId();
		long storeId=info.page.getStoreId();
		cache.remove(pageId);
		List<CacheInfo> storeInfos=storeCache.get(storeId);
		if(storeInfos!=null){
			storeInfos.remove(info);
		}
	}

	/* isPersiste:false PageOutしようとしている時(TODO 将来検討) */
	/* isPersiste:true PageInしてきた時 */
	public boolean put(Page page,boolean isPersiste){
		if(true){
			return false;
		}
		long pageId=page.getPageId();
		long storeId=page.getStoreId();
		
		List<CacheInfo> list=storeCache.get(storeId);
		if(list!=null){
			if(list.size()>=8){//同じStoreのPageは一定数以上覚えない
				return false;
			}
		}
		synchronized(cache){
			CacheInfo cacheInfo=cache.get(pageId);
			if(cacheInfo!=null){
				return false;
			}
			if(list==null){
				list=new ArrayList<CacheInfo>();
				storeCache.put(storeId, list);
			}
			cacheInfo=new CacheInfo();
			//Store全体長
			cacheInfo.totalLength=page.setupCachePage(cacheInfo.page);
			cacheInfo.buffer=page.getBuffer();
			cacheInfo.inTime=System.currentTimeMillis();
			cacheInfo.isPersiste=isPersiste;
			cache.put(pageId, cacheInfo);
			list.add(cacheInfo);
		}
		return true;
	}
	
	/* PageInしようとしている時 */
	public boolean get(Page page){
		if(true){
			return false;
		}
		long pageId=page.getPageId();
		CacheInfo cacheInfo=cache.get(pageId);
		if(cacheInfo==null){
			return false;
		}
		cacheInfo.lastGet=System.currentTimeMillis();
		cacheInfo.count++;
		ByteBuffer[] dupBuffer=PoolManager.duplicateBuffers(cacheInfo.buffer);
		page.putBuffer(dupBuffer);
		return true;
	}
	
	public void onTimer(Object userContext) {
		//追い出すpage数を計算
		
		//cacheを舐めながら、
		//目標のpage数集める
		//storeが有効か調べる
	}

}
