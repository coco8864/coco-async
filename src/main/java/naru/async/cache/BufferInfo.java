package naru.async.cache;

import java.nio.ByteBuffer;

import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

/* buffer��pool�Ŗ��点����,�������L���ȃf�[�^��ێ����Ă���buffer�͐ϋɓI�ɍė��p����|���V�[ */
//default�@Buffer pool�̏󋵂��݂�cache����̒ǂ��o�����s��
public class BufferInfo extends PoolBase{
	public static BufferInfo create(ByteBuffer[] buffer,long totalLength){
		BufferInfo bufferInfo=(BufferInfo)PoolManager.getInstance(BufferInfo.class);
		bufferInfo.init(buffer, totalLength);
		return bufferInfo;
	}
	
	@Override
	public void recycle() {
		if(buffer!=null){
			PoolManager.poolBufferInstance(buffer);
			buffer=null;
		}
	}
	private void init(ByteBuffer[] buffer,long totalLength){
		this.buffer=PoolManager.duplicateBuffers(buffer);
		long length=BuffersUtil.remaining(buffer);
//		this.totalLength=totalLength;
		this.lengthRate=(float)length/(float)totalLength;
		this.cacheTime=System.currentTimeMillis();
	}
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
		float result=0.0f;
		long orgIntervalCount=intervalCount;
		intervalCount=0;
		if(orgIntervalCount>=1){
			return result;
		}
		if(totalCount==0){
			totalCount=1;
		}
		//TODO �������鎖
		return (float)(now-(long)lastTime)/((float)totalCount*lengthRate);
	}
	
	public float getLastScore(){
		return lastScore;
	}
}
