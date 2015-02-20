package naru.async.store4;

import java.nio.ByteBuffer;

public interface PageCallback {
	/**
	 * データ受信側
	 * @param buffer
	 */
	public void onBuffer(ByteBuffer buffer);
	
	/**
	 * Pageエラー発生を通知
	 * @param failure
	 */
	public void onBufferFailure(Throwable failure);
}
