package naru.async.store2;

import java.nio.ByteBuffer;

public interface BufferGetter {
	/**
	 * データ受信側
	 * @param buffers
	 * @param userContext
	 * 
	 * @return 続けて同じパラメタでasyncBufferする場合true
	 */
	public boolean onBuffer(ByteBuffer[] buffers,Object userContext);
	
	/**
	 * 終端を通知
	 * 通知されたStoreは、回収対象となる。(参照数ref関数を呼び出す事で対応可)
	 * @param userContext
	 */
	public void onBufferEnd(Object userContext);
	
	/**
	 * Storeのエラー発生を通知
	 * 通知されたStoreは、回収対象となる。(参照数ref関数を呼び出す事で対応可)
	 * @param failure
	 * @param userContext
	 */
	public void onBufferFailure(Throwable failure,Object userContext);
}
