package naru.async.store2;
/*
 * Store - Page
 *          |
 *         PagePool - Storage
 * 
 */


/**
 * １回に返却するサイズは、Buffer側が性能的に最適なように返す
 * @author Owner
 *
 */
public interface AsyncBuffer {
	/**
	 * buffer側にpostionを持ち順次呼び出す場合に実装
	 * 読み出しは一回きりとなる
	 * @param bufferGetter
	 * @param userContext 任意のオブジェクト,bufferGetterインタフェースのcallbackメソッドに通知される
	 * @return 要求が受け付けられた場合,true
	 */
	public boolean startBuffer(BufferGetter bufferGetter,Object userContext);
	
	public boolean nextBuffer();
	
	public boolean endBuffer();
	
	/**
	 * @return bufferが保持するデータ長を返却、不明な場合は負の値を返却する
	 */
	public long bufferLength();
}
