package naru.async.pool;

import java.awt.image.VolatileImage;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

public class Pool {
	private static Logger logger = Logger.getLogger(Pool.class);
	private static int DEFAULT_POOL_LIMIT = 16;
	/* このpoolに属するlifeを集める */
	// TYPE_BYTE_BUFFERで使用
	private volatile Map<byte[], ByteArrayLife> byteArrayLifes = Collections.synchronizedMap(new HashMap<byte[], ByteArrayLife>());
	// TYPE_POOL_BASE,TYPE_GENERAL,で使用
	private volatile Set<ReferenceLife> poolLifes = Collections.synchronizedSet(new HashSet<ReferenceLife>());
	// TYPE_ARRAYで使用
	//キーに当該オブジェクトそのものを使うので、一般classとarrayはGCされることはない。
	//equals,hashcodeが変化しないオブジェクトに有効
	//開放が漏れるとリーク現象になる
	private volatile Map<Object, ReferenceLife> poolLifesMap = Collections.synchronizedMap(new HashMap<Object, ReferenceLife>());

	ReferenceLife getGeneralReferenceLife(Object obj){
		return getGeneralReferenceLife(obj,false);
	}
	
	ReferenceLife getGeneralReferenceLife(Object obj,boolean remove){
		synchronized(poolLifes){
			Iterator<ReferenceLife> itr=poolLifes.iterator();
			while(itr.hasNext()){
				ReferenceLife life=itr.next();
				if(life.get()==obj){
					if(remove){
						itr.remove();
					}
					return life;
				}
			}
		}
		return null;
	}

	private LinkedList poolStack = new LinkedList();// pool実体
	private static Object[] NO_ARGS = new Object[0];
	private static Class[] NO_TYPES = new Class[0];

	private Class poolClass; // poolされるクラス
	private Constructor poolClassConstructor; // poolClassのコンストラクタ,factoryMethodとスイッチ
	private Method factoryMethod;// ファクトリメソッド、factoryClassNameの指定要、poolClassConstructorとスイッチ
	private Object[] instantiateArgs;// poolClassConstructorもしくは、factoryMethodに渡すパラメタ
	private Method recycleMethod;// recycleメソッド、poolActualClassのメンバメソッドである必要がある。

	static final int TYPE_POOL_BASE = 1;// poolBaseをextendsしたClass
	static final int TYPE_BYTE_BUFFER = 2;// byteBuffer
	static final int TYPE_ARRAY = 3;// 配列
	static final int TYPE_GENERAL = 4;// 一般Class,数個のオブジェクトを使いまわすのに効率
	private int type;

	// private boolean isArray;//配列か否か？
	private int length;// 配列の場合の配列数

	private int initial;// poolの最低数。（起動時に作成）
	private int limit;// poolの最大数。これ以上はpoolしない
	private int increment;// 追加単位

	private long sequence;// 要求数
	private long poolBackCount;// 返却された数
	private long instanceCount;// 作成したinstance数
	private long gcCount;// GCされたオブジェクト数,ByteBufferは、arrayをリサイクル
	private long maxUseCount;
	
	private long getUseCount(){
		return sequence-(poolBackCount+gcCount);
	}	

	private boolean isDelayRecycle = false;// 遅延recycle

	public synchronized ByteArrayLife getByteArrayLife(byte[] array) {
//		synchronized (byteArrayLifes) {
			return byteArrayLifes.get(array);
//		}
	}

	/* 統計情報の返却 */
	public int getInitial() {
		return initial;
	}

	/* limitは任意に書き換えても支障はない */
	public void setLimit(int limit){
		this.limit=limit;
	}
	public int getLimit() {
		return limit;
	}

	public int getIncrement() {
		return increment;
	}

	public long getSequence() {
		return sequence;
	}

	public long getPoolBackCount() {
		return poolBackCount;
	}
	
	public long getPoolCount() {
		return poolStack.size();
	}
	

	public long getInstanceCount() {
		return instanceCount;
	}

	public long getGcCount() {
		return gcCount;
	}

	/* 配列オブジェクトClassに使用 */
	public Pool(Class memberClass, int length, int min, int max, int expansion,
			boolean isDelayRecycle) throws ClassNotFoundException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException,
			InstantiationException {
		this(null, null, memberClass, NO_ARGS, TYPE_ARRAY, length, null, min,
				max, expansion, isDelayRecycle);
	}

	/* PoolBaseをextendsしたClass,パラメタなしのconstractorを持つClassに使用 */
	public Pool(Constructor poolClassConstructor, boolean isExtendsPoolBase,
			String recycleMethodName, int min, int max, int expansion,
			boolean isDelayRecycle) throws ClassNotFoundException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException,
			InstantiationException {
		this(poolClassConstructor, null, null, NO_ARGS,
				isExtendsPoolBase ? TYPE_POOL_BASE : TYPE_GENERAL, 0,
				recycleMethodName, min, max, expansion, isDelayRecycle);
	}

	/* ByteBufferに使用 */
	public Pool(Method factoryMethod, Object[] instantiateArgs,
			String recycleMethodName, int min, int max, int increment)
			throws ClassNotFoundException, SecurityException,
			NoSuchMethodException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException,
			InstantiationException {
		this(null, factoryMethod, null, instantiateArgs, TYPE_BYTE_BUFFER, 0,
				recycleMethodName, min, max, increment, false);
	}

	private Pool(Constructor poolClassConstructor, Method factoryMethod,
			Class poolClass, Object[] instantiateArgs, int type,
			int length,// 配列の場合の配列数
			String recycleMethodName, int initial, int limit, int increment,
			boolean isDelayRecycle) throws ClassNotFoundException,
			SecurityException, NoSuchMethodException, IllegalArgumentException,
			IllegalAccessException, InvocationTargetException,
			InstantiationException {
		this.sequence = this.poolBackCount = this.instanceCount = this.gcCount = 0;
		this.poolClassConstructor = poolClassConstructor;
		if (poolClassConstructor != null) {
			this.poolClass = poolClassConstructor.getDeclaringClass();
		}
		this.factoryMethod = factoryMethod;
		if (factoryMethod != null) {
			this.poolClass = factoryMethod.getReturnType();
		}
		if (this.poolClass == null) {
			this.poolClass = poolClass;
		}
		this.instantiateArgs = instantiateArgs;
		if (recycleMethodName != null) {
			recycleMethod = this.poolClass.getMethod(recycleMethodName,
					NO_TYPES);
		}
		// this.isArray=isArray;
		this.type = type;
		this.length = length;
		this.initial = initial;
		if (limit <= 0) {
			limit = DEFAULT_POOL_LIMIT;
		}
		if (limit < initial) {
			limit = initial;
		}
		this.limit = limit;
		if (increment <= 0) {
			increment = 1;
		}
		this.increment = increment;
		
		addInstance(this.initial);
		
		this.isDelayRecycle = isDelayRecycle;
		this.maxUseCount=0;
	}

	Class getPoolClass() {
		return poolClass;
	}

	Object[] getInstantiateArgs() {
		return instantiateArgs;
	}

	private Class[] setInstantiateArgs(String[] args) {
		Class[] types = new Class[args.length];
		this.instantiateArgs = new Object[args.length];
		for (int i = 0; i < args.length; i++) {
			try {
				int argInt = Integer.parseInt(args[i]);
				types[i] = int.class;
				instantiateArgs[i] = new Integer(argInt);
			} catch (NumberFormatException e) {
				types[i] = String.class;
				instantiateArgs[i] = args[i];
			}
		}
		return types;
	}

	private int addInstance(int count) {
		int addCount = 0;
		for (int i = 0; i < count; i++) {
			if (limit > 0 && poolStack.size() >= limit) {
				logger.warn("pool is full.poolCount:" + instanceCount
						+ ":poolClass:" + poolClass.getName());
				return addCount;
			}
			Object obj = instantiate();
			poolStack.addFirst(obj);
			instanceCount++;
			addCount++;
		}
		return addCount;
	}

	public synchronized void gcLife(ReferenceLife referenceLife) {
		gcCount++;
		poolLifes.remove(referenceLife);
	}

	// poolは、arrayに対するpoolのはず
	/*
	 * byteBufferLifeは、GCされてもarrayから復活させる。GCによりpool数が減ることはない
	public synchronized void gcLife(ByteBufferLife byteBufferLife) {
		gcCount++;
		// arrayLifes.remove(byteBufferLife.getArray());
	}
	*/

	private Object instantiateArray() {
		Object obj = null;
		Object lastObj = null;
		for (int i = 0; i < 4; i++) {// hashCodeが4回同じ値を返却した場合、失敗させる
			lastObj = obj = Array.newInstance(poolClass, length);
			ReferenceLife life = new ReferenceLife(obj);
			life.setPool(this);
			// Integer hashCode=obj.hashCode();
			synchronized (poolLifesMap) {
				ReferenceLife o = poolLifesMap.get(obj);
				if (o == null) {
					poolLifesMap.put(obj, life);
					break;
				}
				// Object o2=o.get();
				// long h=o2.hashCode();
				// long h2=obj.hashCode();
				// logger.warn("instantiateArray hash code
				// retry.hashCode:"+hashCode +":" +(o==obj));
			}
			obj = null;
		}
		if (obj == null) {
			// このArrayオブジェクトは一旦Poolに入るが戻って来たときに管理外と判断される
			logger.warn("instantiateArray retry over,out of pool object");
			obj = lastObj;
		}
		return obj;
	}

	private Object instantiateGeneral() throws IllegalArgumentException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		Object obj = null;
		Object lastObj = null;
		for (int i = 0; i < 4; i++) {// hashCodeが4回同じ値を返却した場合、失敗させる
			if (poolClassConstructor != null) {
				lastObj = obj = poolClassConstructor.newInstance(instantiateArgs);
			} else {
				lastObj = obj = factoryMethod.invoke(null, instantiateArgs);
			}
			ReferenceLife life = new ReferenceLife(obj);
			life.setPool(this);
			// Integer hashCode=obj.hashCode();
			synchronized (poolLifes) {
				poolLifes.add(life);
				break;
				/*
				if (poolLifesMap.get(obj) == null) {
					poolLifesMap.put(obj, life);
					break;
				}
				*/
			}
//			logger.warn("instantiateGeneral hash code retry.hashCode:" + obj);
//			obj = null;
		}
		if (obj == null) {
			// このオブジェクトは一旦Poolに入るが戻って来たときに管理外と判断される
			logger.warn("instantiateGeneral retry over,out of pool object");
			obj = lastObj;
		}
		return obj;
	}

	private Object instantiateByteBuffer() throws IllegalArgumentException,
			IllegalAccessException, InvocationTargetException {
		Object obj = factoryMethod.invoke(null, instantiateArgs);
		// 本当はByteBufferでなくてもseedがポイントできれば管理可能
		ByteBuffer byteBuffer = (ByteBuffer) obj;
		ByteArrayLife arrayLife = new ByteArrayLife(byteBuffer, this);
		byteArrayLifes.put(byteBuffer.array(), arrayLife);
		return obj;
	}

	private Object instantiatePoolBase() throws IllegalArgumentException,
			InstantiationException, IllegalAccessException,
			InvocationTargetException {
		Object obj = null;
		if (poolClassConstructor != null) {
			obj = poolClassConstructor.newInstance(instantiateArgs);
		} else {
			obj = factoryMethod.invoke(null, instantiateArgs);
		}
		PoolBase poolObj = (PoolBase) obj;
		ReferenceLife life = poolObj.getLife();
		life.setPool(this);
		poolLifes.add(life);// 参照を保持しないとlifeがgcされて通知が来なくなる
		if (recycleMethod != null) {
			recycleMethod.invoke(obj, NO_ARGS);
		}
		return obj;
	}

	private Object instantiate() {
		try {
			switch (type) {
			case TYPE_POOL_BASE:
				return (instantiatePoolBase());
			case TYPE_BYTE_BUFFER:
				return (instantiateByteBuffer());
			case TYPE_ARRAY:
				return (instantiateArray());
			case TYPE_GENERAL:
				return (instantiateGeneral());
			}
		} catch (Exception e) {
			logger.error(
					"fail to instantiate.poolClass:" + poolClass.getName(), e);
			throw new RuntimeException("fail to instantiate.poolClass:"
					+ poolClass.getName(), e);
		}
		throw new RuntimeException("fail to instantiate.poolClass:"
				+ poolClass.getName() + ":type:" + type);
	}

	public synchronized Object getInstance() {
		Object obj = null;
		if (poolStack.size() > 0) {
			obj = poolStack.removeFirst();
		}
		if (obj == null) {
			if (addInstance(increment) == 0) {
				logger.error("fail to getInstance." + "poolClass:"
						+ poolClass.getName());
				throw new RuntimeException("fail to getInstance."
						+ "poolClass:" + poolClass.getName());
			}
			return getInstance();
		}
		sequence++;
		long useCount=getUseCount();
		if(useCount>maxUseCount){
			maxUseCount=useCount;
		}
//		logger.debug("getInstance:" + poolClass.getName() + "#" + sequence + "#" + instanceCount);
		ReferenceLife referenceLife=null;
		switch (type) {
		case TYPE_POOL_BASE:
			PoolBase poolObj = (PoolBase) obj;
			poolObj.ref();// 参照数を1にする
			poolObj.setPoolId(sequence);
			poolObj.activate();
			break;
		case TYPE_BYTE_BUFFER:
			ByteBuffer byteBuffer = (ByteBuffer) obj;
			ByteArrayLife byteArrayLife = byteArrayLifes.get(byteBuffer.array());
			obj = byteArrayLife.getFirstByteBuffer(byteBuffer);
			if(byteBuffer!=obj){//ありえないはずだが
				logger.error("TYPE_BYTE_BUFFER　getInstance error.byteBuffer:"+byteBuffer+":obj:"+obj);
				return getInstance();
			}
			break;
		case TYPE_ARRAY:
			referenceLife = poolLifesMap.get(obj);
			referenceLife.ref();
			break;
		case TYPE_GENERAL:
			referenceLife = getGeneralReferenceLife(obj);
			referenceLife.ref();
			break;
		}
//		if(type==TYPE_ARRAY&&poolClass==ByteBuffer.class&&length==1){
//			logger.info("getInstance:bufsid:"+System.identityHashCode(obj));
//		}		
		return obj;
	}

	private void callInactivate(Object obj) {
		if (obj instanceof PoolBase) {
			PoolBase poolObj = (PoolBase) obj;
			try {
				poolObj.inactivate();
			} catch (Exception e) {
				logger.error("fail to inactivate.poolClass:"
						+ poolClass.getName(), e);
				return;
			}
		}
	}

	private void callRecycle(Object obj) {
		if (recycleMethod != null) {
			try {
				recycleMethod.invoke(obj, NO_ARGS);
			} catch (Exception e) {
				logger.error("fail to recyncle.poolClass:"
						+ poolClass.getName(), e);
				return;
			}
		}
	}

	// Poolから溢れて参照を開放するメソッド
	private void releaseLife(Object obj) {
		ReferenceLife referenceLife = null;
		switch (type) {
		case TYPE_POOL_BASE:
			PoolBase poolBase = (PoolBase) obj;
			referenceLife = poolBase.getLife();
			if (referenceLife != null) {
				poolLifes.remove(referenceLife);
				referenceLife.clear();
			}
			break;
		case TYPE_BYTE_BUFFER:
			ByteBuffer byteBuffer = (ByteBuffer) obj;
			ByteArrayLife byteArrayLife = byteArrayLifes.remove(byteBuffer.array());
			if (byteArrayLife != null) {
				byteArrayLife.releaseLife();
				byteArrayLife.clear();
			}else{
				logger.warn("!!ByteBuffer releaseLife4!!");
			}
			break;
		case TYPE_ARRAY:
			referenceLife = poolLifesMap.remove(obj);
			if (referenceLife != null) {
				referenceLife.clear();
			}
			break;
		case TYPE_GENERAL:
			referenceLife = getGeneralReferenceLife(obj,true);
			if (referenceLife != null) {
				referenceLife.clear();
			}
			break;
		}
	}

	public void recycleInstance(Object obj) {
		// if(logger.isDebugEnabled()){
		// if(poolStack.contains(obj)){
		// logger.error("duplicate
		// poolInstance.obj:"+obj+":poolClass:"+poolClass.getName(),new
		// Exception());
		// return;
		// }
		// }
		callRecycle(obj);
		synchronized (this) {
			poolBackCount++;
			if (limit >= 0 && poolStack.size() >= limit) {
				//溢れた場合Poolしない、この場合lifeからの弱参照をclearしないと、gcが通知されてしまう。
				releaseLife(obj);
				return;//溢れた場合はPoolしない
			}
			poolStack.addFirst(obj);
		}
//		logger.debug("recycleInstance:" + poolClass.getName() + "#" + sequence + "#" + instanceCount);
	}

	ReferenceLife getArrayLife(Object obj){
		return poolLifesMap.get(obj);
	}
	
	// typeがARRAY,GENERALの場合呼び出される
	void poolArrayGeneralInstance(Object obj) {
		ReferenceLife life=null;
		if(type==TYPE_ARRAY){
			life = poolLifesMap.get(obj);
		}else if(type==TYPE_GENERAL){
			life = getGeneralReferenceLife(obj);
		}
		if (life == null || life.get() != obj) {
			logger.warn("poolArrayGeneralInstance not in pool.obj:" + obj+":" +life+":"+length +":"+System.identityHashCode(obj),new Exception());
			/*
			 * poolLifesMapの排他をとらないとpoolLifesMap.getがnullを返却する事がある。
			life = poolLifesMap.get(obj);
			if(obj instanceof ByteBuffer[] && length==1){
				ByteBuffer b=((ByteBuffer[])obj)[0];
				if(b!=null){
					logger.warn("poolArrayGeneralInstance peekBuffer ByteBuffer:"+ b);
					b.position(0);
					b.limit(b.capacity());
					BuffersUtil.peekBuffer((ByteBuffer[])obj);
				}
			}
			*/
			return;// 管理外
		}
		if(life.unref()){//正常に開放できた場合poolに戻す
//			if(type==TYPE_ARRAY&&poolClass==ByteBuffer.class&&length==1){
//				logger.info("poolArrayGeneralInstance:bufsid:"+System.identityHashCode(obj));
//			}
			poolInstance(obj);
		}
	}

	public void poolInstance(Object obj) {
		callInactivate(obj);
		if (isDelayRecycle) {// 遅延recycleする場合
			PoolManager.addDerayRecycle(obj);
			return;
		}
		recycleInstance(obj);
	}
	
	/*　ByteBufferPoolの場合,poolに入っているのは代表だけなので放置するとGCされてしまう */
	public void term(){
		Iterator<ByteArrayLife> itr=byteArrayLifes.values().iterator();
		
	}

	public void info() {
		info(false);
	}

	public void info(boolean isDitail) {
		StringBuilder sb = new StringBuilder(poolClass.getName());
		if (type == TYPE_ARRAY) {
			sb.append("[");
			sb.append(length);
			sb.append("]");
		} else if (instantiateArgs != null) {
			sb.append("(");
			for (int i = 0; i < instantiateArgs.length; i++) {
				sb.append(instantiateArgs[i]);
			}
			sb.append(")");
		}
		sb.append(":");
		sb.append(sequence);//通算要求数
		sb.append(":");
		sb.append(instanceCount);//作成数
		sb.append(":");
		sb.append(getUseCount());//現在使用数
		sb.append(":");
		sb.append(getPoolCount());//現在プール数
		sb.append(":");
		sb.append(maxUseCount);
		sb.append(":");
		sb.append(limit);//pool limit
		sb.append(":");
		sb.append(gcCount);//GC数
		// バッファサイズ、総割り当て数、総作成数、使用数
		logger.info(sb.toString());
		Object[] lifes = poolLifes.toArray();
		for (int i = 0; i < lifes.length; i++) {
			ReferenceLife life = (ReferenceLife) lifes[i];
			if (life.getRef() != 0) {
				life.info(isDitail);
			}
		}
		lifes = byteArrayLifes.values().toArray();
		for (int i = 0; i < lifes.length; i++) {
			ReferenceLife life = (ReferenceLife) lifes[i];
			if (life.getRef() != 0) {
				life.info(isDitail);
			}
		}
		Iterator<ReferenceLife> itr= poolLifesMap.values().iterator();
		while(itr.hasNext()){
			ReferenceLife life=itr.next();
			if (life.getRef() != 0) {
				life.info(isDitail);
			}
		}
	}
}
