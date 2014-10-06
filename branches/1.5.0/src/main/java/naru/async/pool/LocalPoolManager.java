package naru.async.pool;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import naru.async.Log;

import org.apache.log4j.Logger;

public class LocalPoolManager {
	private static Logger logger=Logger.getLogger(LocalPoolManager.class);
	private static ThreadLocal<LocalPoolManager> localPool=new ThreadLocal<LocalPoolManager>();
	private static int GARBAGE_LIMIT=128;
	
	private Map<Integer,LocalPool> byteBufferPoolMap=new HashMap<Integer,LocalPool>();
	private Map<Class,Map<Integer,LocalPool>> arrayPoolMap=new HashMap<Class,Map<Integer,LocalPool>>();
	private Map<Class,LocalPool> classPoolMap=new HashMap<Class,LocalPool>();
	
	private LinkedList<PoolBase> unrefObjPool=new LinkedList<PoolBase>();
	private LinkedList<PoolBase> spareUnrefObjPool=new LinkedList<PoolBase>();
	private LinkedList<ByteBuffer> freeByteBufferPool=new LinkedList<ByteBuffer>();
	private LinkedList<ByteBuffer> spareFreeByteBufferPool=new LinkedList<ByteBuffer>();
	private LinkedList<Object> freeArrayPool=new LinkedList<Object>();
	private LinkedList<Object> spareFreeArrayPool=new LinkedList<Object>();
	
	private static LocalPoolManager getLocalPoolManager(){
		if(!PoolManager.useLocalPool()){
			return null;
		}
		LocalPoolManager localPoolManager=localPool.get();
		if(localPoolManager==null){
			localPoolManager=new LocalPoolManager();
			localPool.set(localPoolManager);
			PoolManager.addLocalPoolManager(localPoolManager);
		}
		return localPoolManager;
	}
	
	public static void refresh(){
		LocalPoolManager manager=getLocalPoolManager();
		if(manager==null){
			return;
		}
		if(manager.garbageCount>=GARBAGE_LIMIT){
			Object tmp=manager.freeArrayPool;
			manager.freeArrayPool=manager.spareFreeArrayPool;
			manager.spareFreeArrayPool=(LinkedList<Object>)tmp;
			
			tmp=manager.freeByteBufferPool;
			manager.freeByteBufferPool=manager.spareFreeByteBufferPool;
			manager.spareFreeByteBufferPool=(LinkedList<ByteBuffer>)tmp;
			
			tmp=manager.unrefObjPool;
			manager.unrefObjPool=manager.spareUnrefObjPool;
			manager.spareUnrefObjPool=(LinkedList<PoolBase>)tmp;
			
			PoolManager.enqueuePool(manager);
		}
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
		if(true){//TODO xxx
			return null;
		}
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
		if(true){//TODO xxx
			return null;
		}
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
	
	private LocalPool getArrayPool(Class clazz,int size){
		Map<Integer,LocalPool> am=arrayPoolMap.get(clazz);
		if(am==null){
			return null;
		}
		LocalPool p=am.get(size);
		return p;
	}
	
	public static Object getArrayInstance(Class clazz,int size){
		if(true){//TODO xxx
			return null;
		}
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
	
	public static boolean poolBufferInstance(ByteBuffer buffer) {
		//if(true){//TODO xxx
		//	return false;
		//}
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return false;
		}
		manager.freeByteBufferPool.add(buffer);
		manager.garbageCount++;
		return true;
	}
	
	public static boolean poolBaseUnref(PoolBase obj,boolean isPool) {
		//if(true){//TODO xxx
		//	return false;
		//}
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return false;
		}
		if(isPool){//TODO use isPool
			manager.unrefObjPool.add(obj);
		}else{
			manager.unrefObjPool.add(obj);
		}
		manager.garbageCount++;
		return true;
	}
	
	public static boolean poolArrayInstance(Object objs){
		//if(true){//TODO xxx
		//	return false;
		//}
		LocalPoolManager manager=localPool.get();
		if(manager==null){
			return false;
		}
		manager.freeArrayPool.add(objs);
		manager.garbageCount++;
		return true;
	}
	private String threadName;
	private Thread thread;
	private long lastPause;
	private int pauseCount=0;
	private int garbageCount=0;
	
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
	
	synchronized void recycle(){
		while(!spareUnrefObjPool.isEmpty()){
			PoolBase obj=spareUnrefObjPool.removeFirst();
			obj.unrefInternal(false);
		}
		while(!spareFreeArrayPool.isEmpty()){
			Object objs=spareFreeArrayPool.removeFirst();
			PoolManager.poolArrayInstance(objs,false);
		}
		while(!spareFreeByteBufferPool.isEmpty()){
			ByteBuffer buffer=spareFreeByteBufferPool.removeFirst();
			PoolManager.poolBufferInstance(buffer);
		}
	}
	
	void term(){
		logger.info("LocalPoolManager term["+threadName+"]isAlive:"+thread.isAlive()+":beatCount:"+pauseCount+":last:"+(lastPause-System.currentTimeMillis()));
		recycle();
		while(!unrefObjPool.isEmpty()){
			PoolBase obj=unrefObjPool.removeFirst();
			obj.unrefInternal(false);
		}
		while(!freeArrayPool.isEmpty()){
			Object objs=freeArrayPool.removeFirst();
			PoolManager.poolArrayInstance(objs,false);
		}
		while(!freeByteBufferPool.isEmpty()){
			ByteBuffer buffer=freeByteBufferPool.removeFirst();
			PoolManager.poolBufferInstance(buffer);
		}
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
