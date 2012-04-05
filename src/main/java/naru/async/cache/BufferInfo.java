package naru.async.cache;

import java.nio.ByteBuffer;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;
import naru.async.store.StoreManager;

/* buffer��pool�Ŗ��点����,�������L���ȃf�[�^��ێ����Ă���buffer�͐ϋɓI�ɍė��p����|���V�[ */
//default�@Buffer pool�̏󋵂��݂�cache����̒ǂ��o�����s��
public class BufferInfo extends PoolBase{
	public static BufferInfo create(ByteBuffer[] buffer,long totalLength,FileInfo fileInfo){
		BufferInfo bufferInfo=(BufferInfo)PoolManager.getInstance(BufferInfo.class);
		bufferInfo.init(buffer, totalLength,-1,fileInfo);
		return bufferInfo;
	}
	public static BufferInfo create(ByteBuffer[] buffer,long totalLength,long storeId){
		BufferInfo bufferInfo=(BufferInfo)PoolManager.getInstance(BufferInfo.class);
		bufferInfo.init(buffer, totalLength,storeId,null);
		return bufferInfo;
	}
	
	@Override
	public void recycle() {
		if(buffer!=null){
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
		if(fileInfo!=null){
			fileInfo.unref();
		}
	}
	
	private void init(ByteBuffer[] buffer,long totalLength,long storeId,FileInfo fileInfo){
		this.buffer=PoolManager.duplicateBuffers(buffer);
		long length=BuffersUtil.remaining(buffer);
//		this.totalLength=totalLength;
		this.lengthRate=(float)length/(float)totalLength;
		this.cacheTime=System.currentTimeMillis();
		this.storeId=storeId;
		if(fileInfo!=null){
			fileInfo.ref(false);
		}
		this.fileInfo=fileInfo;
	}
	private long storeId;
	private FileInfo fileInfo;
	
	private ByteBuffer[] buffer;
	/* ���v��� */
//	private long length;//���Y�o�b�t�@�̒���
//	private long totalLength;//�f�[�^�S�̂̒���,
	
	private float lengthRate;
	private long cacheTime;
	private long lastTime;
	private int totalCount;
	private int intervalCount;//���ꂪ1�ȏ�̏ꍇ�ǂ��o���Ȃ�
	
	public ByteBuffer[] duplicateBuffers(){
		return PoolManager.duplicateBuffers(buffer);
	}
	
	public void ref(){
		totalCount++;
		intervalCount++;
		lastTime=System.currentTimeMillis();
		super.ref();
	}
	
	private float lastScore=0.0f;
	
	/* ���Yinfo�̕s�v�x(�傫���Ǝ̂Ă���\��������) */
	public float getScore(long now){
		float lastScore=0.0f;
		long orgIntervalCount=intervalCount;
		intervalCount=0;
		if(orgIntervalCount>=1){
			return lastScore;
		}
		if(totalCount==0){
			totalCount=1;
		}
		//TODO �������鎖
		lastScore=(float)(now-(long)lastTime)/((float)totalCount*lengthRate);
		return lastScore;
	}
	
	public float getLastScore(){
		return lastScore;
	}
	public boolean isChange() {
		if(fileInfo!=null){
			return fileInfo.isChange();
		}
		long totalLength=StoreManager.getStoreLength(storeId);
		return(totalLength<0);
	}
}
