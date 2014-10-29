package naru.async.store2;

import java.nio.ByteBuffer;

public interface BufferGetter {
	/**
	 * �f�[�^��M��
	 * @param buffers
	 * @param userContext
	 * 
	 * @return �����ē����p�����^��asyncBuffer����ꍇtrue
	 */
	public boolean onBuffer(ByteBuffer[] buffers,Object userContext);
	
	/**
	 * �I�[��ʒm
	 * �ʒm���ꂽStore�́A����ΏۂƂȂ�B(�Q�Ɛ�ref�֐����Ăяo�����őΉ���)
	 * @param userContext
	 */
	public void onBufferEnd(Object userContext);
	
	/**
	 * Store�̃G���[������ʒm
	 * �ʒm���ꂽStore�́A����ΏۂƂȂ�B(�Q�Ɛ�ref�֐����Ăяo�����őΉ���)
	 * @param failure
	 * @param userContext
	 */
	public void onBufferFailure(Throwable failure,Object userContext);
}
