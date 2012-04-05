package naru.async.cache;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import naru.async.Timer;
import naru.async.pool.PoolManager;
import naru.async.timer.TimerManager;

public class FileCache implements Timer{
	private static Logger logger=Logger.getLogger(FileCache.class);
	private static final long INTERVAL=10000;
	private static FileCache instance=new FileCache();
	public static FileCache getInstance(){
		return instance;
	}
	private FileCache(){
		this.timer=TimerManager.setInterval(INTERVAL, this, null);
	}
	private Object timer;
	private int min=512;
	private int max=min*2;
	private int overFlow=0;
	private int hit=0;
	private int miss=0;
	
	private Map<File,FileInfo> cache=new HashMap<File,FileInfo>();
	
	//check中はtmpCacheに入れておく
	private boolean isCheck=false; 
	private Map<File,FileInfo> tmpCache=new HashMap<File,FileInfo>();
	
	public void setCacheSize(int min){
		this.min=min;
		this.max=min*2;
	}
	
	public FileInfo createFileInfo(File file){
		FileInfo result=(FileInfo)PoolManager.getInstance(FileInfo.class);
		result.init(file);
		return result;
	}
	
	public FileInfo get(File file){
		FileInfo result=cache.get(file);
//		intervalGet++;
		if(result!=null){
			result.ref();
			hit++;
			return result;
		}
		miss++;
		result=createFileInfo(file);
		if(cache.size()>max){
			overFlow++;
			return result;
		}
//		intervalPut++;
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
	
	private float scoreThreshold=Float.MAX_VALUE;
	private ArrayList<Float> scores=new ArrayList<Float>();
	
	private boolean check(FileInfo fileInfo,long now){
		if(fileInfo.isChange()==false){//変更があるか?
			return false;
		}
		float lastScore=fileInfo.getLastScore();
		float score=fileInfo.getScore(now);
		if(lastScore>scoreThreshold&&score>lastScore){
			return false;
		}
		//TODO 全部入ったとしても、minを超えない場合は、addしないようにする
		scores.add(score);
		return true;
	}
	
	public void term(){
		TimerManager.clearInterval(timer);
		logger.info("FileCache term: size:"+cache.size()+":min:"+min+":hit:"+hit+":miss:"+miss+":overFlow:"+overFlow);
		synchronized(this){
			Iterator<FileInfo> itr=cache.values().iterator();
			while(itr.hasNext()){
				FileInfo fileInfo=itr.next();
				itr.remove();
				fileInfo.unref();
			}
			Iterator<FileInfo> tmpItr=tmpCache.values().iterator();
			while(tmpItr.hasNext()){
				FileInfo fileInfo=itr.next();
				itr.remove();
				fileInfo.unref();
			}
			max=0;
		}
	}
	
	public void onTimer(Object userContext) {
		synchronized(this){
			isCheck=true;
		}
		long now=System.currentTimeMillis();
		Iterator<FileInfo> itr=cache.values().iterator();
		while(itr.hasNext()){
			FileInfo fileInfo=itr.next();
			if(!check(fileInfo,now)){
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
		if(scores.size()<min){
			scoreThreshold=Float.MAX_VALUE;
		}else{
			Collections.sort(scores);
			scoreThreshold=scores.get(min);
		}
		scores.clear();
	}
}
