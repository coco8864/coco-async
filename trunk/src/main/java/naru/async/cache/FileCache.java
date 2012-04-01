package naru.async.cache;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import naru.async.Timer;
import naru.async.pool.PoolManager;
import naru.async.timer.TimerManager;

public class FileCache implements Timer{
	private static final long INTERVAL=10000;
	private static FileCache instance=new FileCache();
	public static FileCache getInstance(){
		return instance;
	}
	private FileCache(){
		this.timer=TimerManager.setInterval(INTERVAL, this, null);
	}
	private Object timer;
	private int min=128;
	private int max=512;
	private int overFlow=0;
	private int hit=0;
	private int miss=0;
	private int intervalGet=0;
	private int intervalPut=0;
	
	private Map<File,FileInfo> cache=new HashMap<File,FileInfo>();
	
	//check中はtmpCacheに入れておく
	private boolean isCheck=false; 
	private Map<File,FileInfo> tmpCache=new HashMap<File,FileInfo>();
	
	public FileInfo get(File file){
		FileInfo result=cache.get(file);
		intervalGet++;
		if(result!=null){
			result.ref();
			hit++;
			return result;
		}
		miss++;
		result=(FileInfo)PoolManager.getInstance(FileInfo.class);
		result.init(file);
		if(cache.size()>max){
			overFlow++;
			return result;
		}
		intervalPut++;
		synchronized(this){
			if(!isCheck){
				cache.put(file, result);
			}else{
				tmpCache.put(file, result);
			}
		}
		result.ref();
		return result;
	}
	
	//前回のチェックでの削除候補
	//今回チェックの削除候補
	private boolean check(FileInfo fileInfo){
		if(fileInfo.check()==false){//変更があるか?
			return false;
		}
		int count=fileInfo.getIntervalCount();
		if(count!=0){
			return true;
		}
		return true;
	}
	
	public void onTimer(Object userContext) {
		int getCount;
		int putCount;
		synchronized(this){
			isCheck=true;
			getCount=intervalGet;
			putCount=intervalPut;
			intervalGet=intervalPut=0;
		}
		Iterator<FileInfo> itr=cache.values().iterator();
		while(itr.hasNext()){
			FileInfo fileInfo=itr.next();
			if(check(fileInfo)){
				itr.remove();
				fileInfo.unref();
				continue;
			}
		}
		synchronized(this){
			isCheck=false;
			cache.putAll(tmpCache);
			tmpCache.clear();
		}
	}
}
