package naru.async.cache;

import java.nio.ByteBuffer;

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
		this.totalLength=totalLength;
		this.cacheTime=System.currentTimeMillis();
	}
	private ByteBuffer[] buffer;
	/* ���v��� */
	private long totalLength;//�S�̂̒���,
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
	
	public int intervalReset(){
		int orgIntervalCount=intervalCount;
		intervalCount=0;
		return orgIntervalCount;
	}
}
