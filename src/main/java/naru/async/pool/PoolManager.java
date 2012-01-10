package naru.async.pool;

import java.lang.ref.ReferenceQueue;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import naru.async.Timer;
import naru.async.timer.TimerManager;
import naru.queuelet.Queuelet;
import naru.queuelet.QueueletContext;

/**
 * ByteBufferのライフサイクルを考える
 * 1)getBufferInstance()で獲得
 * 2)duplicateBufferでduplicate
 * 3)1)2)ともpoolBufferInstance()で返却
 * 
 * ChannelContext毎に使用できるByteBuffer数を制限する手はないか？
 * @author naru
 */
public class PoolManager implements Queuelet,Timer{
	private static Logger logger=Logger.getLogger(PoolManager.class);
	private static final int ARRAY_SIZE_COUNT=32;//同一配列クラスは、サイズによって32種類まで
	private static final int ARRAY_MAX_POOL_COUNT=16;//配列クラスは、これ以上Poolしない
	private static final Object[] NO_ARGS=new Object[0];
	private static final Class[] NO_TYPES=new Class[0];
	
	private static PoolManager instance;
	private static QueueletContext queueletContext;
	private static int defaultBufferSize=16384;
	
	private Map<Class,Pool> classPoolMap=new HashMap<Class,Pool>();
	private Map<Integer,Pool> byteBufferPoolMap=new HashMap<Integer,Pool>();
	private Map<Class,Map<Integer,Pool>> arrayPoolMap=new HashMap<Class,Map<Integer,Pool>>();
	private Map<Class,Object> array0PoolMap=new HashMap<Class,Object>();

    private ReferenceQueue referenceQueue = new ReferenceQueue();
	private List<Class> delayRecycleClasses=new ArrayList<Class>();
	private LinkedList delayRecycleArray=new LinkedList();
	
	private static void setupPool(Pool pool,int limit){
		long poolCount=pool.getPoolCount();
		pool.setLimit(limit);
		for(long i=(long)limit;i<poolCount;i++){
			Object o=pool.getInstance();
			pool.recycleInstance(o);
		}
	}
	
	public static void setupClassPool(String className,int limit){
		Class clazz=null;
		try {
			clazz = findClass(className);
		} catch (ClassNotFoundException e) {
			logger.error("fail to setupClassPool."+className,e);
			return;
		}
		Pool pool=getClassPool(clazz);
		if(pool==null){
			pool=addClassPool(clazz);
		}
		setupPool(pool,limit);
	}
	
	public static void setupBufferPool(int size,int limit){
		Pool pool=getBufferPool(size);
		if(pool==null){
			pool=addBufferPool(size);
		}
		setupPool(pool,limit);
	}
	
	public static void setupArrayPool(String className,int size,int limit){
		Class clazz=null;
		try {
			clazz = findClass(className);
		} catch (ClassNotFoundException e) {
			logger.error("fail to setupArrayPool."+className,e);
			return;
		}
		Pool pool=addArrayPool(clazz,size);
		setupPool(pool,limit);
	}
	
	/* defaultBufferを変更する */
	public static void changeDefaultBuffer(int updateDefaultBufferSize,int limit){
		if(defaultBufferSize==updateDefaultBufferSize){
			setupBufferPool(updateDefaultBufferSize,limit);
			return;
		}
		int orgDefaultBufferSize=defaultBufferSize;
		defaultBufferSize=updateDefaultBufferSize;
		setupBufferPool(updateDefaultBufferSize,limit);
		setupBufferPool(orgDefaultBufferSize,0);
	}
	
	/* 統計情報の返却 */
	public static Pool getBufferPool(int size) {
		return instance.byteBufferPoolMap.get(size);
	}
	
	public static void createBufferPool(int size,int limit){
		Pool pool=addBufferPool(size);
		pool.setLimit(limit);
	}
	
	///
	public static Pool getClassPool(Class clazz) {
		return instance.classPoolMap.get(clazz);
	}
	public static Pool getArrayPool(Class clazz,int size){
		Map<Integer,Pool> am=instance.arrayPoolMap.get(clazz);
		if(am==null){
			return null;
		}
		Pool p=am.get(size);
		return p;
	}
	
	public static void createArrayPool(Class clazz,int size,int limit){
		Pool pool=addArrayPool(clazz,size);
		pool.setLimit(limit);
	}
	
    public static ReferenceQueue getReferenceQueue(){
    	if(instance==null){
    		return null;
    	}
    	return instance.referenceQueue;
    }
    
    public static void dump(){
    	instance.dumpPool();
    }
    
    public static void addDerayRecycle(Object obj){
    	synchronized(instance.delayRecycleArray){
    		instance.delayRecycleArray.add(obj);
    	}
    }
    
    private static boolean isDelayRecycleClass(Class clazz){
    	Iterator<Class> itr=instance.delayRecycleClasses.iterator();
    	while(itr.hasNext()){
    		Class baseClass=itr.next();
    		if(baseClass.isAssignableFrom(clazz)){
    			return true;
    		}
    	}
    	return false;
    }
    
	private static void delayRecycleObject(Object obj){
		Pool pool=null;
		if(obj instanceof PoolBase){
			PoolBase poolObj=(PoolBase)obj;
			pool=(Pool)poolObj.getLife().getPool();
		}else{
			pool=(Pool)instance.classPoolMap.get(obj.getClass());
		}
		if(pool!=null){
			pool.recycleInstance(obj);
		}
	}
    
	private static void delayRecycleObjects(Object checkObj){
		List list=new ArrayList();
		synchronized(instance.delayRecycleArray){
			Iterator itr=instance.delayRecycleArray.iterator();
			while(itr.hasNext()){
				Object obj=itr.next();
				itr.remove();
				if(obj==checkObj){//自分自身を番兵として入れる
					break;
				}
				list.add(obj);
			}
		}
		Iterator itr=list.iterator();
		while(itr.hasNext()){
			Object obj=itr.next();
			itr.remove();
			delayRecycleObject(obj);
		}
		if(checkObj==null && instance.delayRecycleArray.size()>0){
			delayRecycleObjects(checkObj);
		}
	}
    
	private static Pool addArrayPool(Class clazz,int size){
		try {
			synchronized(instance.arrayPoolMap){
				Map<Integer,Pool> m=instance.arrayPoolMap.get(clazz);
				if(m==null){
					m=new HashMap<Integer,Pool>();
					instance.arrayPoolMap.put(clazz, m);
				}
				if(m.size()>=ARRAY_SIZE_COUNT){
					return null;
				}
				if(m.get(size)!=null){//既に登録されていた
					logger.warn("aleady exist pool.clazz:"+clazz.getName()+":size:"+size);
					return m.get(size);
//					return null;
				}
				Pool pool=new Pool(
						clazz,size,
						1,ARRAY_MAX_POOL_COUNT,1,false);
				m.put(size, pool);
				return pool;
			}
		} catch (Exception e) {
			logger.error("fail to addClassPool.clazz:"+clazz.getName(),e);
			throw new IllegalStateException("fail to addClassPool.clazz:"+clazz.getName(),e);
		}
	}
	
	private static Pool addClassPool(Class clazz){
		try {
			boolean isExtendsPoolBase=false;
			String recycleMethodName=null;
			if( PoolBase.class.isAssignableFrom(clazz) ){
				recycleMethodName="recycle";
				isExtendsPoolBase=true;
			}
			Pool pool=new Pool(
				clazz.getConstructor(NO_TYPES),isExtendsPoolBase,
				recycleMethodName,
				1,-1,1,isDelayRecycleClass(clazz));
			synchronized(instance.classPoolMap){
				instance.classPoolMap.put(clazz, pool);
			}
			return pool;
		} catch (Exception e) {
			logger.error("fail to addClassPool.clazz:"+clazz.getName(),e);
			throw new IllegalStateException("fail to addClassPool.clazz:"+clazz.getName(),e);
		}
	}
	
	private static Pool addBufferPool(int bufferSize) {
		try {
			Pool pool=new Pool(
					ByteBuffer.class.getMethod("allocate", new Class[]{Integer.TYPE}),
					new Object[]{new Integer(bufferSize)},
					"clear",
					1,-1,1);
			synchronized(instance.byteBufferPoolMap){
				Pool orgPool=instance.byteBufferPoolMap.get(bufferSize);
				if(orgPool!=null){
					return orgPool;
				}
				instance.byteBufferPoolMap.put(bufferSize, pool);
			}
			return pool;
		} catch (Exception e) {
			logger.error("fail to addBufferPool.bufferSize:"+bufferSize,e);
			throw new IllegalStateException("fail to addBufferPool.bufferSize:"+bufferSize,e);
		}
	}
    
	/**
	 * pool待ち合わせへのキューイング
	 * @param obj
	 */
	public static void enqueuePool(Object obj){
		if(obj==null){
			return;
		}
		queueletContext.enque(obj);
	}
	
	/* TODO 配列プール
	 * poolにあるかもしれないObjectを取得する場合に呼び出すメソッド
	 */
	public static Object getArrayInstance(Class clazz,int size){
//		System.out.println("logger:"+logger.getClass().getClassLoader().toString());
//		Thread.dumpStack();
		if(size==0){
			//sizeが0の場合、状態がないので同じオブジェクトを返却すればよい
			Object obj=instance.array0PoolMap.get(clazz);
			if(obj==null){
				obj=Array.newInstance(clazz, 0);
				synchronized(instance.array0PoolMap){
					instance.array0PoolMap.put(clazz, obj);
				}
			}
			return obj;
		}
		Pool pool=getArrayPool(clazz,size);
		if(pool==null){
			pool=addArrayPool(clazz, size);
			if(pool==null){
				logger.warn("new Array instance."+clazz.getName()+":length:"+size);
				return Array.newInstance(clazz, size);
			}
		}
		return pool.getInstance();
	}
	
	public static void poolArrayInstance(Object objs){
		Class clazz=objs.getClass();
		int arraySize=Array.getLength(objs);
		if(arraySize==0){
			return;
		}
		Pool pool=getArrayPool(clazz.getComponentType(), arraySize);
		if(pool==null){
			logger.warn("isn't pool instance."+clazz.getName()+":length:"+Array.getLength(objs));
			/* いきなり返してきたパターンは、再利用しない */
			return;
		}
		pool.poolArrayGeneralInstance(objs);
	}
	
	/*
	 * poolにあるかもしれないObjectを取得する場合に呼び出すメソッド
	 */
	public static Object getInstance(Class clazz){
    	if(instance==null){
    		try {
				return clazz.newInstance();
			} catch (InstantiationException e) {
				logger.error("fail to getInstance.className:"+clazz.getName());
				throw new RuntimeException("fail to getInstance.className:"+clazz.getName());
			} catch (IllegalAccessException e) {
				logger.error("fail to getInstance.className:"+clazz.getName());
				throw new RuntimeException("fail to getInstance.className:"+clazz.getName());
			}
    	}
		Pool pool=(Pool)instance.classPoolMap.get(clazz);
		if(pool==null){
			pool=addClassPool(clazz);
		}
		Object obj=pool.getInstance();
		if(obj==null){
			logger.error("fail to getInstance.className:"+clazz.getName());
			throw new RuntimeException("fail to getInstance.className:"+clazz.getName());
		}
		return obj;
	}
	

	/**
	 * 即座にpoolに戻したい場合に呼び出すメソッド
	 * PoolBaseをextendsしたオブジェクトは、unrefメソッドを呼ぶこと。
	 */
	public static void poolInstance(Object obj){
		Pool pool=null;
		if(obj instanceof PoolBase){
			logger.warn("poolInstance PoolBase",new Exception());
			PoolBase poolBase=(PoolBase)obj;
			poolBase.unref(true);
			return;
		}
		Class clazz=obj.getClass();
		if(logger.isDebugEnabled()){
			if(clazz.isArray()){
				logger.error("poolInstance array",new Exception());
				poolArrayInstance(obj);
				return;
			}
		}
		pool=(Pool)instance.classPoolMap.get(obj.getClass());
		if(pool==null){
			logger.warn("isn't pool instance."+clazz.getName());
			/* いきなり返してきたパターンは、再利用しない */
			return;
		}
		pool.poolArrayGeneralInstance(obj);
	}
	
	private long recycleInterval=60000;
//	private long watchIntervalCount=1;
//	private long timerId;
	private Object interval;
	
	private String[] getArgs(Map param,String poolName){
		List argsList=new ArrayList();
		for(int i=0;;i++){
			String arg=(String)param.get(poolName + ".factoryArg" + (i+1));
			if(arg==null){
				break;
			}
			argsList.add(arg);
		}
		return (String [])argsList.toArray(new String[0]);
	}
	
	private static Class findClass(String className) throws ClassNotFoundException{
		if("byte".equals(className)){
			return byte.class;
		}else if("int".equals(className)){
			return int.class;
		}else if("short".equals(className)){
			return short.class;
		}else if("long".equals(className)){
			return long.class;
		}else if("char".equals(className)){
			return char.class;
		}else if("float".equals(className)){
			return float.class;
		}else if("double".equals(className)){
			return double.class;
		}
		return Class.forName(className);
	}
	
	/* 起動時に実行 */
	private void createPool(Map param,String name) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException{
		String className=(String)param.get(name + ".className");
		if(className==null){
			return;
		}
		boolean isExtendsPoolBase=false;
		String recycleMethodName=(String)param.get(name + ".recycleMethod");
		
		Class poolClass=findClass(className);
		if(PoolBase.class.isAssignableFrom(poolClass)){
			recycleMethodName="recycle";
			isExtendsPoolBase=true;
		}
		int initial=0,limit=-1,increment=1;
		String initialString=(String)param.get(name + ".initial");
		String limitString=(String)param.get(name + ".limit");
		String incrementString=(String)param.get(name + ".increment");
		if(initialString!=null){
			initial=Integer.parseInt(initialString);
		}
		if(limitString!=null){
			limit=Integer.parseInt(limitString);
		}
		if(incrementString!=null){
			increment=Integer.parseInt(incrementString);
		}
		
		String arrayLengthParam=(String)param.get(name + ".arrayLength");
		if(arrayLengthParam==null){
			Pool pool=new Pool(
					poolClass.getConstructor(NO_TYPES),isExtendsPoolBase,
					recycleMethodName,
					initial,limit,increment,isDelayRecycleClass(poolClass));
			classPoolMap.put(poolClass, pool);
		}else{
			int arrayLength=Integer.parseInt(arrayLengthParam);
			Pool pool=new Pool(poolClass,arrayLength,initial,limit,increment,false);
			Map<Integer,Pool> m=arrayPoolMap.get(poolClass);
			if(m==null){
				m=new HashMap<Integer,Pool>();
				instance.arrayPoolMap.put(poolClass, m);
			}
			m.put(arrayLength, pool);
		}
	}
	
	private void createBufferPool(Map param,String name) throws SecurityException, IllegalArgumentException, ClassNotFoundException, NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		String bufferSizeParam=(String)param.get(name + ".bufferSize");
		if(bufferSizeParam==null){
			logger.warn("failt to createBufferPool.name:"+name);
			return;
		}
		int bufferSize=Integer.parseInt(bufferSizeParam);
		if("default".equals(name)){
			defaultBufferSize=bufferSize;
		}
		int initial=0,limit=-1,increment=1;
		String initialString=(String)param.get(name + ".initial");
		String limitString=(String)param.get(name + ".limit");
		String incrementString=(String)param.get(name + ".increment");
		if(initialString!=null){
			initial=Integer.parseInt(initialString);
		}
		if(limitString!=null){
			limit=Integer.parseInt(limitString);
		}
		if(incrementString!=null){
			increment=Integer.parseInt(incrementString);
		}
		Pool pool=new Pool(
				ByteBuffer.class.getMethod("allocate", new Class[]{Integer.TYPE}),
				new Object[]{new Integer(bufferSize)},
				"clear",
				initial,limit,increment);
		byteBufferPoolMap.put(bufferSize,pool);
	}
	
	public void init(QueueletContext context, Map param) {
		PoolManager.instance=this;
		PoolManager.queueletContext=context;
		try {
			//遅延回収クラスを取得（クラス名としたのは、継承がある場合一括して設定できるメリットを選択）
			String delayRecycleClassesNames=(String)param.get("delayRecycleClasses");
			if(delayRecycleClassesNames!=null){
				String[] delayRecycleClassesNameArray=delayRecycleClassesNames.split(",");
				for(int i=0;i<delayRecycleClassesNameArray.length;i++){
					Class clazz=Class.forName(delayRecycleClassesNameArray[i]);
					delayRecycleClasses.add(clazz);
				}
			}
			
//			String watchIntervalCountString=(String)param.get("watchIntervalCount");
//			if(watchIntervalCountString!=null){
//				watchIntervalCount=Long.parseLong(watchIntervalCountString);
//			}
			
			String recycleIntervalString=(String)param.get("recycleInterval");
			if(recycleIntervalString!=null){
				recycleInterval=Long.parseLong(recycleIntervalString);
			}
			String poolNames=(String)param.get("poolNames");
			if(poolNames!=null){
				String[] poolNameArray=poolNames.split(",");
				for(int i=0;i<poolNameArray.length;i++){
					createPool(param,poolNameArray[i]);
				}
			}
			String poolBuffers=(String)param.get("poolBuffers");
			if(poolBuffers!=null){
				String[] poolBuffersArray=poolBuffers.split(",");
				for(int i=0;i<poolBuffersArray.length;i++){
					createBufferPool(param,poolBuffersArray[i]);
				}
			}
		} catch (Throwable e) {
			logger.error("PoolManger init error.",e);
			context.finish();
			return;
		}
		addDerayRecycle(delayRecycleArray);//最初の番兵を登録
		interval=TimerManager.setInterval(recycleInterval, this, "PoolManagerTimer");
	}

	public boolean service(Object req) {
		poolInstance(req);
		return true;
	}

	private boolean isStop=false;
	public void term() {
		TimerManager.clearInterval(interval);
		logger.info("===PoolManager term start===");
//		Page.saveFreePage(0);
		onTimer(null);
		delayRecycleObjects(null);
		dumpPool(true);
		logger.info("===PoolManager term end===");
	}
	
	private void dumpPool(){
		dumpPool(false);
	}

	private void dumpPool(boolean isDetail){
		Object[] pools=classPoolMap.values().toArray();
		for(int i=0;i<pools.length;i++){
			Pool pool=(Pool)pools[i];
			pool.info(isDetail);
		}
		pools=byteBufferPoolMap.values().toArray();
		for(int i=0;i<pools.length;i++){
			Pool pool=(Pool)pools[i];
			pool.info(isDetail);
		}
		Object[] arrayPools=arrayPoolMap.values().toArray();
		for(int i=0;i<arrayPools.length;i++){
			Map poolsm=(Map)arrayPools[i];
			pools=poolsm.values().toArray();
			for(int j=0;j<pools.length;j++){
				Pool pool=(Pool)pools[j];
				pool.info(isDetail);
			}
		}
	}
	
	
	private long timerCount=0;
	public void onTimer(Object userContext) {
		logger.debug("PoolManager onTimer.interval:"+interval);
		timerCount++;
		try{
			delayRecycleObjects(delayRecycleArray);//遅延回収の実行
			/* ByteBufferがGCされた場合は、ArrayLifeに保存されているbyte[]を再利用する */
			/* その他のオブジェクトは警告を出力するだけ */
			while(true){
				ReferenceLife life=(ReferenceLife) referenceQueue.poll();
				if(life==null){
					break;
				}
				life.gcInstance();
			}
//			if(timerCount%watchIntervalCount==0){
//				dumpPool();
//				ChannelContext.dumpChannelContexts();
//			}
		}finally{
			addDerayRecycle(delayRecycleArray);//時間毎に番兵を登録
		}
	}
	
	/**
	 * 以降buffer関連のメソッド,BufferPoolにデリケート
	 * @return
	 */
	public static ByteBuffer getBufferInstance() {
		return getBufferInstance(getDefaultBufferSize());
	}
	
	public static int getDefaultBufferSize(){
		return defaultBufferSize;
	}
	
//	public static void setDefaultBufferSize(int defaultBufferSize){
//		PoolManager.defaultBufferSize=defaultBufferSize;
//	}
	
	private static final ByteBuffer ZERO_BUFFER=ByteBuffer.allocate(0);
	
	public static ByteBuffer getBufferInstance(int bufferSize) {
		if(bufferSize==0){
			return ZERO_BUFFER;
		}
		bufferSize=(((bufferSize-1)/(1024))+1)*1024;//1024の倍数に調整する
		Pool pool=null;
//		synchronized(instance.byteBufferPoolMap){
			pool=instance.byteBufferPoolMap.get(bufferSize);
//		}
		if(pool==null){
			pool=addBufferPool(bufferSize);
		}
		ByteBuffer buffer=(ByteBuffer)pool.getInstance();
//		if(bufferSize==defaultBufferSize){
//			logger.info("getBufferInstance:"+buffer.array(),new Exception());
//		}
		return buffer;
	}
	
	public static ByteBuffer duplicateBuffer(ByteBuffer buffer) {
		return duplicateBuffer(buffer,false);
	}
	
	public static ByteBuffer duplicateBuffer(ByteBuffer buffer,boolean isNewBuffer) {
		if(buffer==null){
			return null;
		}
		byte[] array=buffer.array();
		int length=array.length;
		if(isNewBuffer){
			ByteBuffer newBuffer=PoolManager.getBufferInstance(length);
			byte[] newArray=newBuffer.array();
			System.arraycopy(array, 0, newArray, 0, length);
			newBuffer.position(buffer.position());
			newBuffer.limit(buffer.limit());
			return newBuffer;
		}
		
		Pool pool=null;
//		synchronized(instance.byteBufferPoolMap){
			pool=instance.byteBufferPoolMap.get(length);
//		}
//		if(true){
		if(pool==null){//pool管理外
			return buffer.duplicate();
		}
		ByteArrayLife arrayLife=pool.getByteArrayLife(array);
		if(arrayLife==null){//pool管理外
			return buffer.duplicate();
		}
		ByteBuffer dupBuffer=arrayLife.getByteBuffer();
		dupBuffer.position(buffer.position());
		dupBuffer.limit(buffer.limit());
		return dupBuffer;
	}

	public static ByteBuffer[] duplicateBuffers(ByteBuffer buffer[]) {
		return duplicateBuffers(buffer,false);
	}
	
	public static ByteBuffer[] duplicateBuffers(ByteBuffer buffer[],boolean isNewBuffer) {
		ByteBuffer[] dupBuffers=BuffersUtil.newByteBufferArray(buffer.length);
		for(int i=0;i<buffer.length;i++){
			dupBuffers[i]=duplicateBuffer(buffer[i],isNewBuffer);
		}
		return dupBuffers;
	}

	/**
	 * 値の決まったbyte配列に対するByteBufferをPool管理する
	 */
	private static final int CONST_POOL_SIZE=8;
	private static Map<byte[],List<ByteBuffer>> constByteBufferPool=new HashMap<byte[],List<ByteBuffer>>();
	
	public static ByteBuffer getConstBuffer(byte[] array){
		List<ByteBuffer> constPool=constByteBufferPool.get(array);
		if(constPool==null){
			constPool=new ArrayList<ByteBuffer>();
			synchronized(constByteBufferPool){
				constByteBufferPool.put(array,constPool);
			}
		}
		ByteBuffer buffer=null;
		synchronized(constPool){
			if(constPool.size()>0){
				buffer=constPool.remove(0);
			}
		}
		if(buffer==null){
			return ByteBuffer.wrap(array);
		}
		buffer.rewind();
		return buffer;
	}
	
	public static void poolBufferInstance(ByteBuffer buffer) {
		if(buffer==null||buffer==ZERO_BUFFER){
			return;
		}
		byte[] array=buffer.array();
		int length=array.length;
		/*
		List<ByteBuffer> constPool=constByteBufferPool.get(array);
		if(constPool!=null){
			synchronized(constPool){
				if(constPool.size()<CONST_POOL_SIZE){
					constPool.add(buffer);
				}
			}
			return;
		}
		*/
		Pool pool=null;
//		synchronized(instance.byteBufferPoolMap){
			pool=instance.byteBufferPoolMap.get(length);
//		}
		if(pool==null){//pool管理外
			if(length==16384){
				logger.warn("poolBufferInstance 1:"+array);
			}
			return;
		}
		ByteArrayLife arrayLife=pool.getByteArrayLife(array);
		if(arrayLife==null){//pool管理外
			if(length==16384){
				logger.warn("poolBufferInstance 2:"+array +":"+pool);
			}
			return;
		}
		arrayLife.poolByteBuffer(buffer);
	}

	public static void poolBufferInstance(ByteBuffer[] buffers) {
		if(buffers==null){
			return;
		}
		for(int i=0;i<buffers.length;i++){
			poolBufferInstance(buffers[i]);
		}
		poolArrayInstance(buffers);//配列を再利用する
	}
	
	public static void poolBufferInstance(List<ByteBuffer> buffers) {
		if(buffers==null){
			return;
		}
		Iterator<ByteBuffer> itr=buffers.iterator();
		while(itr.hasNext()){
			ByteBuffer buffer=itr.next();
			poolBufferInstance(buffer);
			itr.remove();
		}
	}
}
