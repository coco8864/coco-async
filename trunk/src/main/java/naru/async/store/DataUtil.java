package naru.async.store;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import naru.async.pool.PoolManager;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
/**
 * digestのbase64は、こんな感じ,25byte
 * 7ggmZ1r6JMnw5IInSb2j9Q==
 * js5QK4XgYvAvazRMt9ac8A==
 * g1ZYmtFg0Xh9LW95FvOSPg==
 * 
 * @author naru
 *
 */
public class DataUtil {
	private static Logger logger=Logger.getLogger(DataUtil.class);
	private static MessageDigest messageDigestMD5;
	private static MessageDigest messageDigestSHA1;
	private static MessageDigest messageDigestSHA256;
	static{
		try {
			messageDigestMD5=MessageDigest.getInstance("MD5");
			messageDigestSHA1=MessageDigest.getInstance("SHA1");
			messageDigestSHA256=MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			logger.error("MessageDigest.getInstance error.",e);
		}
	}
	
	public static String digest(MessageDigest messageDigest){
		byte[] digestByte;
		digestByte=messageDigest.digest();
		return encodeBase64(digestByte);
	}
	
	public static String digest(byte[] data){
		if( data==null ){
			return null;
		}
		byte[] digestByte;
		synchronized(messageDigestMD5){
			messageDigestMD5.reset();
			digestByte=messageDigestMD5.digest(data);
		}
		return encodeBase64(digestByte);
	}
	
	public static boolean equalsDigest(String digest,byte[] data){
		if( digest==null && data==null ){
			return true;
		}
		if( digest==null || data==null ){
			return false;
		}
		return digest.equals(digest(data));
	}
	
	public static String digestHex(byte[] data){
		if( data==null ){
			return null;
		}
		byte[] digestByte;
		synchronized(messageDigestMD5){
			messageDigestMD5.reset();
			digestByte=messageDigestMD5.digest(data);
		}
		return byteToString(digestByte);
	}
	
	public static String digestHexSha1(byte[] data){
		if( data==null ){
			return null;
		}
		byte[] digestByte;
		synchronized(messageDigestSHA1){
			messageDigestSHA1.reset();
			digestByte=messageDigestSHA1.digest(data);
		}
		return byteToString(digestByte);
	}
	
	public static String digestHexSha256(byte[] data){
		if( data==null ){
			return null;
		}
		byte[] digestByte;
		synchronized(messageDigestSHA256){
			messageDigestSHA1.reset();
			digestByte=messageDigestSHA256.digest(data);
		}
		return byteToString(digestByte);
	}
	
	public static String digestBase64Sha1(byte[] data){
		if( data==null ){
			return null;
		}
		byte[] digestByte;
		synchronized(messageDigestSHA1){
			messageDigestSHA1.reset();
			digestByte=messageDigestSHA1.digest(data);
		}
		return encodeBase64(digestByte);
	}
	
	
    private static final char[] hexmap = {
    	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    	'a', 'b', 'c', 'd', 'e', 'f'};
	
	public static String byteToString(byte[] bytes){
		return byteToString(bytes,0,bytes.length);
	}
	
	public static String byteToString(byte[] bytes,int pos,int length){
        char[] buffer = new char[length*2];
        for (int i=0; i<length; i++) {
            int low = (int) (bytes[pos+i] & 0x0f);
            int high = (int) ((bytes[pos+i] & 0xf0) >> 4);
            buffer[i*2] = hexmap[high];
            buffer[i*2 + 1] = hexmap[low];
        }
        return new String(buffer);
	}

	public static String encodeBase64(String text) {
		return encodeBase64(text,"iso8859_1");
	}
	
	public static String encodeBase64(byte[] data) {
		try {
			byte[] bytes = Base64.encodeBase64(data);
			return new String(bytes, "iso8859_1");
		} catch (UnsupportedEncodingException e) {
			logger.error("unkown enc:iso8859_1", e);
			throw new RuntimeException("unkown enc:iso8859_1", e);
		}
	}
	
	public static String encodeBase64(String text,String enc) {
		try {
			return encodeBase64(text.getBytes(enc));
		} catch (UnsupportedEncodingException e) {
			logger.error("unkown enc:"+enc, e);
			throw new RuntimeException("unkown enc:"+enc, e);
		}
	}
	
	public static String decodeBase64(String text) {
		return decodeBase64(text,"iso8859_1");
	}
	
	public static byte[] decodeBase64Bytes(String text) {
		try {
			return Base64.decodeBase64(text.getBytes("iso8859_1"));
		} catch (UnsupportedEncodingException e) {
			logger.error("unkown enc:iso8859_1", e);
			throw new RuntimeException("unkown enc:iso8859_1", e);
		}
	}

	public static String decodeBase64(String text,String enc) {
		try {
			byte[] bytes = decodeBase64Bytes(text);
			return new String(bytes, enc);
		} catch (UnsupportedEncodingException e) {
			logger.error("unkown enc:"+enc, e);
			throw new RuntimeException("unkown enc:"+enc, e);
		}
	}
	
	/*
	 * READモードのStoreを一機に読み込んでByteBuffer配列に変換する
	 */
	public static ByteBuffer[] toByteBuffers(Store store){
		if(store==null){
			return null;
		}
		StoreCollector collector=(StoreCollector) PoolManager.getInstance(StoreCollector.class);
		ByteBuffer[] buffers=collector.collect(store);
		collector.unref(true);
		return buffers;
	}
	
	public static ByteBuffer[] toByteBuffers(long storeId){
		Store store=Store.open(storeId);
		return toByteBuffers(store);
	}
	public static ByteBuffer[] toByteBuffers(String digest){
		Store store=Store.open(digest);
		return toByteBuffers(store);
	}
	
}
