package naru.async.pool;

import org.apache.log4j.Logger;

public class PoolBase {
	private static Logger logger=Logger.getLogger(PoolBase.class);
	
	//�֌W����I�u�W�F�N�g�Q���i�[���A�v�[���ɖ߂��^�C�~���O�̓������Ƃ�
	private long poolId=-1;//�v�[���Ǘ����犄�蓖�Ă�A���̃I�u�W�F�N�g���C�t�T�C�N���ɑ΂����ӂȔԍ�
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
	 * ���YObject���Q�Ƃ��鎞�ɌĂяo��
	 */
	public void ref(){
		life.ref();
	}
	
	/**
	 * ���YObject���Q�Ƃ��Ȃ����鎞�ɌĂяo��
	 * �Q�Ɛ���0�ɂȂ�����Pool�ɕԋp����
	 * pool����̃`�F�b�N�����Ȃ�
	 */
	public boolean unref(){
		return unref(false);
	}
	
	/**
	 * ���YObject���Q�Ƃ��Ȃ����鎞�ɌĂяo��
	 * �Q�Ɛ���0�ɂȂ����ꍇPool�ɕԋp����
	 * pool����̃`�F�b�N�����{
	 * 
	 * @param isPool pool�ɕԋp���ׂ�(true)/�s��(false)
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
			//pool�Ǘ�����Ă��Ȃ�
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
	 * pool�ԋp�O�̌㏈���A�����o�������̂��߂ɌĂяo�����B
	 */
	public void recycle(){}
	
	/**
	 * �v�[�����̔r�����ŌĂ΂��
	 * �������ꂽ�S�I�u�W�F�N�g���Ď����邽�߂Ɏg�p�ł���
	 */
	public void activate(){}
	/**
	 * �v�[�����̔r�����ŌĂ΂��
	 * �������ꂽ�S�I�u�W�F�N�g���Ď����邽�߂Ɏg�p�ł���
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
