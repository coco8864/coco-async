package naru.async;

import java.nio.ByteBuffer;

public interface BufferGetter {
	/**
	 * データ受信側
	 * 
	 * @param userContext
	 * @param buffers
	 * @return 続けて同じパラメタでasyncBufferする場合true
	 */
	public boolean onBuffer(Object userContext,ByteBuffer[] buffers);
	
	/**
	 * 終端を通知
	 * 通知されたStoreは、回収対象となる。(参照数ref関数を呼び出す事で対応可)
	 * @param userContext
	 */
	public void onBufferEnd(Object userContext);
	
	/**
	 * Storeのエラー発生を通知
	 * 通知されたStoreは、回収対象となる。(参照数ref関数を呼び出す事で対応可)
	 * 
	 * @param userContext
	 * @param failure
	 */
	public void onBufferFailure(Object userContext,Throwable failure);
}
