package naru.async.store4;

import java.nio.ByteBuffer;

public interface PageCallback {
	/**
	 * �f�[�^��M��
	 * @param buffer
	 * @param userContext
	 * 
	 * @return �����ē����p�����^��asyncBuffer����ꍇtrue
	 */
	public boolean onBuffer(ByteBuffer buffer,Object userContext);
	
	/**
	 * Page�G���[������ʒm
	 * @param failure
	 * @param userContext
	 */
	public void onBufferFailure(Throwable failure,Object userContext);
}
