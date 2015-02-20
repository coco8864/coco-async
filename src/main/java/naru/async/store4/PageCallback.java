package naru.async.store4;

import java.nio.ByteBuffer;

public interface PageCallback {
	/**
	 * �f�[�^��M��
	 * @param buffer
	 */
	public void onBuffer(ByteBuffer buffer);
	
	/**
	 * Page�G���[������ʒm
	 * @param failure
	 */
	public void onBufferFailure(Throwable failure);
}
