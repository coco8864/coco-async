package naru.async.pool;

import org.apache.log4j.Logger;

public class PoolBase {
	private static Logger logger=Logger.getLogger(PoolBase.class);
	
	//関係するオブジェクト群を格納し、プールに戻すタイミングの同期をとる
	private long poolId=-1;//プール管理から割り当てる、そのオブジェクトライフサイクルに対する一意な番号
	private ReferenceLife life=new ReferenceLife(this);
	
	public ReferenceLife getLife(){
		return life;
	}
	void setPoolId(long poolId){
		this.poolId=poolId;
	}
	public long getPoolId(){
		return poolId;
	}
	
	/**
	 * 当該Objectを参照する時に呼び出し
	 */
	public void ref(){
		life.ref();
	}
	
	/**
	 * 当該Objectを参照しなくする時に呼び出し
	 * 参照数が0になった時Poolに返却する
	 * pool動作のチェックをしない
	 */
	public boolean unref(){
		return unref(false);
	}
	
	/**
	 * 当該Objectを参照しなくする時に呼び出し
	 * 参照数が0になった場合Poolに返却する
	 * pool動作のチェックを実施
	 * 
	 * @param isPool poolに返却すべき(true)/不明(false)
	 */
	public boolean unref(boolean isPool){
		if(LocalPoolManager.poolBaseUnref(this, isPool)){
			return isPool;
		}
		return unrefInternal(isPool);
	}
	
	boolean unrefInternal(boolean isPool){
		Pool pool=life.getPool();
		if(pool==null){
			//pool管理されていない
			if(isPool){
				logger.error("unref(treu) but not pool control",new Throwable());
				return false;
			}
			return true;
		}
		boolean result=true;
		if( life.unref() ){
			pool.poolInstance(this);
		}else if(isPool){
			logger.error("unref(treu) but not poolInstance",new Throwable());
			result=false;
		}
		return result;
	}
	
	/*
	 * pool返却前の後処理、メンバ初期化のために呼び出される。
	 */
	public void recycle(){}
	
	/**
	 * プール毎の排他中で呼ばれる
	 * 生成された全オブジェクトを監視するために使用できる
	 */
	public void activate(){}
	/**
	 * プール毎の排他中で呼ばれる
	 * 生成された全オブジェクトを監視するために使用できる
	 */
	public void inactivate(){}
	
	
	public String toString() {
		return getClass().getName()+":" +getPoolId() + ":ref:" + life.getRef()+":"+super.toString();
	}
	
	protected int getRef(){
		return life.getRef();
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (poolId ^ (poolId >>> 32));
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final PoolBase other = (PoolBase) obj;
		if (poolId != other.poolId)
			return false;
		return true;
	}
	
}
