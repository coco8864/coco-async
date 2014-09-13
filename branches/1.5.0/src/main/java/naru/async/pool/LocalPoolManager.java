package naru.async.pool;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import naru.async.Log;

import org.apache.log4j.Logger;

public class LocalPoolManager {
	private static Logger logger=Logger.getLogger(LocalPoolManager.class);
	private static ThreadLocal<LocalPoolManager> localPool=new ThreadLocal<LocalPoolManager>();
	private class LocalPool{
		public LocalPool(Pool pool) {
			this.pool=pool;
		}
		Pool pool;
		LinkedList<Object> freePool=new LinkedList<Object>();
		LinkedList<Object> usedPool=new LinkedList<Object>();
		int getCount;
		int poolCount;
		int hit;
		int max;
		int total;
		void beat(){
			Log.debug(logger, "LocalPool beat.getCount:",getCount,":poolCount:",poolCount,":hit:",hit,":max:",max);
			pool.batchPool(usedPool);
			pool.batchGet(freePool,max);
			if(max<getCount){
				max=getCount;
			}
			hit=getCount=poolCount=0;
		}
	}
	private Map<Integer,LocalPool> byteBufferPoolMap=new HashMap<Integer,LocalPool>();
	private Map<Class,Map<Integer,LocalPool>> arrayPoolMap=new HashMap<Class,Map<Integer,LocalPool>>();
	private LinkedList<PoolBase> unrefObjPool=new LinkedList<PoolBase>();
	
	private static LocalPoolManager get(){
		LocalPoolManager ppt=localPool.get();
		if(ppt==null){
			ppt=new LocalPoolManager();
			localPool.set(ppt);
		}
		return ppt;
	}
	
	public static void refresh(){
		//TODO need or not
		//Thread.yield();
		LocalPoolManager manager=get();
		manager.beat();
	}
	
	public static ByteBuffer getBufferInstance(int bufferSize) {
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return null;
		}
		LocalPool localPool=manager.byteBufferPoolMap.get(bufferSize);
		if(localPool==null){
			return null;
		}
		localPool.getCount++;
		if(localPool.freePool.size()==0){
			return null;
		}
		localPool.hit++;
		return (ByteBuffer)localPool.freePool.removeFirst();
	}
	
	public static boolean poolBufferInstance(ByteBuffer buffer) {
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return false;
		}
		int bufferSize=buffer.capacity();
		LocalPool localPool=manager.byteBufferPoolMap.get(bufferSize);
		if(localPool==null){
			return false;
		}
		localPool.poolCount++;
		localPool.usedPool.add(buffer);
		return true;
	}
	
	public static boolean poolBaseUnref(PoolBase obj,boolean isPool) {
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return false;
		}
		if(isPool){//TODO use isPool
			manager.unrefObjPool.add(obj);
		}else{
			manager.unrefObjPool.add(obj);
		}
		return true;
	}
	
	private LocalPool getArrayPool(Class clazz,int size){
		Map<Integer,LocalPool> am=arrayPoolMap.get(clazz);
		if(am==null){
			return null;
		}
		LocalPool p=am.get(size);
		return p;
	}
	
	public static Object getArrayInstance(Class clazz,int size){
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return null;
		}
		LocalPool localPool=manager.getArrayPool(clazz, size);
		if(localPool==null){
			return null;
		}
		localPool.getCount++;
		if(localPool.freePool.size()==0){
			return null;
		}
		localPool.hit++;
		return localPool.freePool.removeFirst();
	}
	
	public static boolean poolArrayInstance(Object objs){
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return false;
		}
		Class clazz=objs.getClass();
		int arraySize=Array.getLength(objs);
		if(arraySize==0){
			return true;
		}
		LocalPool localPool=manager.getArrayPool(clazz, arraySize);
		if(localPool==null){
			return false;
		}
		localPool.poolCount++;
		localPool.usedPool.add(objs);
		return true;
	}
	private String threadName;
	private long lastRefresh;
	private int beatCount=0;
	
	private LocalPoolManager(){
		Log.debug(logger, "LocalPoolManager");
		threadName=Thread.currentThread().getName();
		PoolManager.setupLocalPoolManager(this);
	}
	
	void registerByteBufferPool(Pool pool,int bufferlength){
		LocalPool localPool=new LocalPool(pool);
		byteBufferPoolMap.put(bufferlength, localPool);
	}
	
	void registerArrayPool(Pool pool,Class clazz,int size){
		LocalPool localPool=new LocalPool(pool);
		Map<Integer,LocalPool> m=arrayPoolMap.get(clazz);
		if(m==null){
			m=new HashMap<Integer,LocalPool>();
			arrayPoolMap.put(clazz, m);
		}
		m.put(size, localPool);
	}
	
	private void beat(){
		beatCount++;
		Log.debug(logger, "beat.beatCount:",beatCount,":unrefObjPool.size():"+unrefObjPool.size());
		for(Integer bufferlength:byteBufferPoolMap.keySet()){
			LocalPool pool=byteBufferPoolMap.get(bufferlength);
			pool.beat();
		}
		for(Class clazz:arrayPoolMap.keySet()){
			Map<Integer,LocalPool> pools=arrayPoolMap.get(clazz);
			for(Integer size:pools.keySet()){
				LocalPool pool=pools.get(size);
				pool.beat();
			}
		}
		while(!unrefObjPool.isEmpty()){
			PoolBase obj=unrefObjPool.removeFirst();
			obj.unref2(false);
			
		}
	}
}
