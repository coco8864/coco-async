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
	private static int PAUSE_INTERVAL_COUNT=1024;
	private static int PAUSE_INTERVAL_TIME=1000;
	
	private Map<Integer,LocalPool> byteBufferPoolMap=new HashMap<Integer,LocalPool>();
	private Map<Class,Map<Integer,LocalPool>> arrayPoolMap=new HashMap<Class,Map<Integer,LocalPool>>();
	private Map<Class,LocalPool> classPoolMap=new HashMap<Class,LocalPool>();
	private LinkedList<PoolBase> unrefObjPool=new LinkedList<PoolBase>();
	private LinkedList<PoolBase> spareUnrefObjPool=new LinkedList<PoolBase>();
	
	private static LocalPoolManager get(){
		if(!PoolManager.useLocalPool()){
			return null;
		}
		LocalPoolManager localPoolManager=localPool.get();
		if(localPoolManager==null){
			localPoolManager=new LocalPoolManager();
			localPool.set(localPoolManager);
			PoolManager.addLocalPoolManager(localPoolManager);
		}
		localPoolManager.refCount++;
		return localPoolManager;
	}
	
	public static void refresh(){
		//TODO need or not
		//Thread.yield();
		LocalPoolManager manager=get();
		manager.pause();
	}
	
	public static void end(){
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return;
		}
		manager.term();
		localPool.remove();
	}
	
	
	public static Object getInstance(Class clazz) {
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return null;
		}
		LocalPool localPool=manager.classPoolMap.get(clazz);
		if(localPool==null){
			Pool pool=PoolManager.getClassPool(clazz);
			if(pool==null){
				return null;
			}
			localPool=manager.registerClassPool(pool, clazz);
		}
		return localPool.get();
	}
	
	public static ByteBuffer getBufferInstance(int bufferSize) {
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return null;
		}
		LocalPool localPool=manager.byteBufferPoolMap.get(bufferSize);
		if(localPool==null){
			Pool pool=PoolManager.getBufferPool(bufferSize);
			if(pool==null){
				return null;
			}
			localPool=manager.registerByteBufferPool(pool,bufferSize);
		}
		return (ByteBuffer)localPool.get();
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
		localPool.pool(buffer);
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
			Pool pool=PoolManager.getArrayPool(clazz, size);
			if(pool==null){
				return null;
			}
			localPool=manager.registerArrayPool(pool,clazz, size);
		}
		return localPool.get();
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
		localPool.pool(objs);
		return true;
	}
	private String threadName;
	private Thread thread;
	private long lastPause;
	private int pauseCount=0;
	private int refCount=0;/* beatä‘Ç≈éQè∆ÇµÇΩâÒêî */
	
	private LocalPoolManager(){
		Log.debug(logger, "LocalPoolManager");
		thread=Thread.currentThread();
		threadName=Thread.currentThread().getName();
	}
	
	private LocalPool registerByteBufferPool(Pool pool,int bufferlength){
		LocalPool localPool=new LocalPool(pool);
		byteBufferPoolMap.put(bufferlength, localPool);
		return localPool;
	}
	
	private LocalPool registerArrayPool(Pool pool,Class clazz,int size){
		LocalPool localPool=new LocalPool(pool);
		Map<Integer,LocalPool> m=arrayPoolMap.get(clazz);
		if(m==null){
			m=new HashMap<Integer,LocalPool>();
			arrayPoolMap.put(clazz, m);
		}
		m.put(size, localPool);
		return localPool;
	}
	
	private LocalPool registerClassPool(Pool pool,Class clazz){
		LocalPool localPool=new LocalPool(pool);
		classPoolMap.put(clazz, localPool);
		return localPool;
	}
	
	private void pause(){
		long now=System.currentTimeMillis();
		if(refCount<PAUSE_INTERVAL_COUNT&&(now-lastPause)<PAUSE_INTERVAL_TIME){
			return;
		}
		Log.debug(logger, "pause.beatCount:",pauseCount,":unrefObjPool.size():"+unrefObjPool.size());
		for(Integer bufferlength:byteBufferPoolMap.keySet()){
			LocalPool pool=byteBufferPoolMap.get(bufferlength);
			pool.pause();
		}
		for(Class clazz:arrayPoolMap.keySet()){
			Map<Integer,LocalPool> pools=arrayPoolMap.get(clazz);
			for(Integer size:pools.keySet()){
				LocalPool pool=pools.get(size);
				pool.pause();
			}
		}
		for(Class clazz:classPoolMap.keySet()){
			LocalPool pool=classPoolMap.get(clazz);
			pool.pause();
		}
		synchronized(this){
			if(spareUnrefObjPool.size()==0){
				LinkedList<PoolBase> tmp=spareUnrefObjPool;
				spareUnrefObjPool=unrefObjPool;
				unrefObjPool=tmp;
				PoolManager.enqueuePool(this);
			}
		}
		while(!unrefObjPool.isEmpty()){
			PoolBase obj=unrefObjPool.removeFirst();
			obj.unrefInternal(false);
		}
		pauseCount++;
		lastPause=System.currentTimeMillis();
		refCount=0;
	}
	
	synchronized void charge(){
		while(!spareUnrefObjPool.isEmpty()){
			PoolBase obj=spareUnrefObjPool.removeFirst();
			obj.unrefInternal(false);
		}
	}
	
	void term(){
		logger.info("LocalPoolManager term["+threadName+"]isAlive:"+thread.isAlive()+":beatCount:"+pauseCount+":last:"+(lastPause-System.currentTimeMillis()));
		for(Integer bufferlength:byteBufferPoolMap.keySet()){
			LocalPool pool=byteBufferPoolMap.get(bufferlength);
			pool.term();
		}
		for(Class clazz:arrayPoolMap.keySet()){
			Map<Integer,LocalPool> pools=arrayPoolMap.get(clazz);
			for(Integer size:pools.keySet()){
				LocalPool pool=pools.get(size);
				pool.term();
			}
		}
		for(Class clazz:classPoolMap.keySet()){
			LocalPool pool=classPoolMap.get(clazz);
			pool.term();
		}
		while(!unrefObjPool.isEmpty()){
			PoolBase obj=unrefObjPool.removeFirst();
			obj.unrefInternal(false);
		}
		logger.info("LocalPoolManager term["+threadName+"]end");
	}

	public void info() {
		logger.info("LocalPoolManager info["+threadName+"]isAlive:"+thread.isAlive()+":beatCount:"+pauseCount+":last:"+(lastPause-System.currentTimeMillis()));
		for(Integer bufferlength:byteBufferPoolMap.keySet()){
			LocalPool pool=byteBufferPoolMap.get(bufferlength);
			pool.info();
		}
		for(Class clazz:arrayPoolMap.keySet()){
			Map<Integer,LocalPool> pools=arrayPoolMap.get(clazz);
			for(Integer size:pools.keySet()){
				LocalPool pool=pools.get(size);
				pool.info();
			}
		}
		for(Class clazz:classPoolMap.keySet()){
			LocalPool pool=classPoolMap.get(clazz);
			pool.info();
		}
		logger.info("unrefObjPool.size:"+unrefObjPool.size());
		logger.info("LocalPoolManager info["+threadName+"]end");
	}
}
