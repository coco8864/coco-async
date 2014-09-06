package naru.async.pool;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class LocalPoolManager {
	private static ThreadLocal<LocalPoolManager> localPool=new ThreadLocal<LocalPoolManager>();
	private class LocalPool{
		int size;
		LinkedList<Object> freePool=new LinkedList<Object>();
		LinkedList<Object> usedPool=new LinkedList<Object>();
		int getCount;
		int poolCount;
		int hit;
		int max;
		int total;
	}
	private Map<Integer,LocalPool> byteBufferPoolMap=new HashMap<Integer,LocalPool>();
	private Map<Class,Map<Integer,LocalPool>> arrayPoolMap=new HashMap<Class,Map<Integer,LocalPool>>();
	
	private static LocalPoolManager get(){
		LocalPoolManager ppt=localPool.get();
		if(ppt==null){
			ppt=new LocalPoolManager();
			localPool.set(ppt);
		}
		return ppt;
	}
	
	public static void refresh(){
		LocalPoolManager manager=get();
		manager.fresh();
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
		return (ByteBuffer)localPool.freePool.getFirst();
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
		return localPool.freePool.getFirst();
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
	private int refreshCount=0;
	
	private LocalPoolManager(){
		threadName=Thread.currentThread().getName();
		PoolManager.setupLocalPoolManager(this);
	}
	
	private void fresh(){
		refreshCount++;
		
		
	}
}
