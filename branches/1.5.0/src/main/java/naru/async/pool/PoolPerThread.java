package naru.async.pool;

import java.nio.ByteBuffer;

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
	}
	
	public static boolean poolBufferInstance(ByteBuffer buffer) {
		
	}
	
	public static Object getArrayInstance(Class clazz,int size){
		
	}
	
	public static boolean poolArrayInstance(Object objs){
	}
	
	private String ThreadName;
	private int refreshCount=0;
	private class Pool{
		
	}
	
	

}
