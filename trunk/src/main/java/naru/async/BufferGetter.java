package naru.async;

import java.nio.ByteBuffer;

public interface BufferGetter {
	/**
	 * �f�[�^��M��
	 * 
	 * @param userContext
	 * @param buffers
	 * @return �����ē����p�����^��asyncBuffer����ꍇtrue
	 */
	public boolean onBuffer(Object userContext,ByteBuffer[] buffers);
	
	/**
	 * �I�[��ʒm
	 * �ʒm���ꂽStore�́A����ΏۂƂȂ�B(�Q�Ɛ�ref�֐����Ăяo�����őΉ���)
	 * @param userContext
	 */
	public void onBufferEnd(Object userContext);
	
	/**
	 * Store�̃G���[������ʒm
	 * �ʒm���ꂽStore�́A����ΏۂƂȂ�B(�Q�Ɛ�ref�֐����Ăяo�����őΉ���)
	 * 
	 * @param userContext
	 * @param failure
	 */
	public void onBufferFailure(Object userContext,Throwable failure);
}
