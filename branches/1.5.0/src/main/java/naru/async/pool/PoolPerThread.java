package naru.async.pool;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class PoolPerThread {
	private static ThreadLocal<PoolPerThread> threadLocal=new ThreadLocal<PoolPerThread>();
	
	private static PoolPerThread get(){
		PoolPerThread ppt=threadLocal.get();
		if(ppt==null){
			ppt=new PoolPerThread();
			ppt.ThreadName=Thread.currentThread().getName();
			threadLocal.set(ppt);
		}
		return ppt;
	}
	
	public static void refresh(){
		PoolPerThread pool=get();
		pool.refreshCount++;
	}
	
	public static ByteBuffer getBufferInstance(int bufferSize) {
		PoolPerThread pool=get();
	}
	
	public static boolean poolBufferInstance(ByteBuffer buffer) {
		PoolPerThread pool=get();
		int bufferSize=buffer.capacity();
		Counter counter=pool.bufferPool.get(bufferSize);
		counter.usedPool.add(buffer);
		counter.poolCount++;
	}
	
	public static Object getArrayInstance(Class clazz,int size){
		PoolPerThread pool=get();
		
	}
	
	public static boolean poolArrayInstance(Object objs){
		PoolPerThread pool=get();
	}
	
	private String ThreadName;
	private long lastRefresh;
	private int refreshCount=0;
	private class Counter{
		int size;
		LinkedList<ByteBuffer> freePool=new LinkedList<ByteBuffer>();
		LinkedList<ByteBuffer> usedPool=new LinkedList<ByteBuffer>();
		int getCount;
		int poolCount;
		int hit;
		int max;
		int total;
	}
	private Map<Integer,Counter> bufferPool=new HashMap<Integer,Counter>();
	private Map<Integer,Counter> ArrayPool=new HashMap<Integer,Counter>();
}
