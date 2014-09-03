package naru.async.pool;

public class PoolPerThread {
	private static ThreadLocal<PoolPerThread> tl=new ThreadLocal<PoolPerThread>();
	
	private static PoolPerThread get(){
		PoolPerThread ppt=tl.get();
		if(ppt==null){
			ppt=new PoolPerThread();
			ppt.ThreadName=Thread.currentThread().getName();
			tl.set(ppt);
		}
		return ppt;
	}
	
	private String ThreadName;
	
	

}
