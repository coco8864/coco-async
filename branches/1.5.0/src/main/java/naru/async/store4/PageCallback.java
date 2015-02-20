package naru.async.store4;

import java.nio.ByteBuffer;

public interface PageCallback {
	/**
	 * データ受信側
	 * @param buffer
	 * @param userContext
	 * 
	 * @return 続けて同じパラメタでasyncBufferする場合true
	 */
	public boolean onBuffer(ByteBuffer buffer,Object userContext);
	
	/**
	 * Pageエラー発生を通知
	 * @param failure
	 * @param userContext
	 */
	public void onBufferFailure(Throwable failure,Object userContext);
}
