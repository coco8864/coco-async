package naru.async.cache;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import naru.async.BufferGetter;
import naru.async.pool.BuffersUtil;
import naru.async.pool.PoolBase;
import naru.async.pool.PoolManager;

/**
 * IO�𔭐����ɗ͔�����|���V
 * read�Ɋւ��Ă͏����A�N�Z�X���T�|�[�g
 * @author Owner
 *
 */
public class AsyncFile extends PoolBase{
	private static FileCache fileCache=FileCache.getInstance();
	private static BufferCache bufferCache=BufferCache.getInstance();
	
	public static AsyncFile open(File file){
		AsyncFile asyncFile=(AsyncFile)PoolManager.getInstance(AsyncFile.class);
		asyncFile.fileInfo=fileCache.get(file);
		return asyncFile;
	}
	
	@Override
	public void recycle() {
		if(fileInfo!=null){
			fileInfo.unref();
			fileInfo=null;
		}
	}

	private FileInfo fileInfo;
	/* ���͂œǂݍ��ޏꍇ�ɗ��p���� */
	private java.nio.channels.FileChannel fileChannel;
	private long position=0;
	
	public boolean isDirectory() {
		return fileInfo.isDirectory();
	}

	public boolean isFile() {
		return fileInfo.isFile();
	}

	public boolean isCanRead() {
		return fileInfo.isCanRead();
	}

	public boolean isExists() {
		return fileInfo.isExists();
	}

	public long getLastModified() {
		return fileInfo.getLastModified();
	}

	public long getLength() {
		return fileInfo.getLength();
	}

	//�������g����̂́Alisting�@�\�̂݁A���̕��ׂ��팸���������listing�@�\��off�ɂ���
	public File[] listFiles() {
		return fileInfo.listFiles();
	}

	/* callback����asyncRead���Ăяo���̂͋֎~,�f�[�^����������Γ��삷�邪�A
	 * �傫���Ȃ�Ăяo���K�w�����e�ł��Ȃ����傫���Ȃ� */
	public synchronized boolean asyncRead(BufferGetter bufferGetter,Object userContext){
		//cache�ɑ��݂��邩�H
		ByteBuffer[] buffer=bufferCache.get(fileInfo,position);
		if(buffer!=null){
			bufferGetter.onBuffer(userContext, buffer);
			position+=BuffersUtil.remaining(buffer);
			return true;
		}
		ByteBuffer dst=null;
		int length;
		try {
			if(fileChannel==null){
				FileInputStream fis=new FileInputStream(fileInfo.getFile());
				fileChannel=fis.getChannel();
			}
			dst = PoolManager.getBufferInstance();
			length = fileChannel.read(dst);
		} catch (IOException e) {
			bufferGetter.onBufferEnd(userContext);
			return true;
		}
		dst.flip();
		buffer=BuffersUtil.toByteBufferArray(dst);
		position+=(long)length;
		//cache�ɓo�^
		bufferCache.put(fileInfo,position,buffer);
		bufferGetter.onBuffer(userContext, buffer);
		return true;
	}
	
	public void close(){
		if(fileChannel==null){
			return;
		}
		try {
			fileChannel.close();
		} catch (IOException ignore) {
		}
		unref();
	}
}
