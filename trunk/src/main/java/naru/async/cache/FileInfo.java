package naru.async.cache;

import java.io.File;

import naru.async.pool.PoolBase;

public class FileInfo extends PoolBase{
	private File file;
	private boolean exists;
	private boolean isDirectory;
	private boolean isFile;
	private boolean canRead;
	private long lastModified;
	private long length;
	
	/* ìùåvèÓïÒ */
	private long cacheInTime;
	private long lastTime;
	private int totalCount;
	private int intervalCount;
	
	public void init(File file){
		this.file=file;
		this.exists=file.exists();
		this.isDirectory=file.isDirectory();
		this.isFile=file.isFile();
		this.canRead=file.canRead();
		this.lastModified=file.lastModified();
		this.length=file.length();
		this.cacheInTime=System.currentTimeMillis();
	}
	public File getFile() {
		return file;
	}
	public boolean isExists() {
		return exists;
	}
	public boolean isDirectory() {
		return isDirectory;
	}
	public boolean isFile() {
		return isFile;
	}
	public boolean isCanRead() {
		return canRead;
	}
	public long getLastModified() {
		return lastModified;
	}
	public long getLength() {
		return length;
	}
	public File[] listFiles(){
		return file.listFiles();
	}
	public long getCacheInTime() {
		return cacheInTime;
	}
	public long getLastTime() {
		return lastTime;
	}
	public int getTotalCount() {
		return totalCount;
	}
	public int getIntervalCount() {
		return intervalCount;
	}
	
	public boolean check(){
		boolean nowExist=file.exists();
		if(exists){
			if(!nowExist){
				return false;
			}
			if(file.lastModified()!=lastModified){
				return false;
			}
		}else{
			if(nowExist){
				return false;
			}
		}
		return true;
	}
	
	public void ref(){
		totalCount++;
		intervalCount++;
		lastTime=System.currentTimeMillis();
		super.ref();
	}
	
	public int intervalReset(){
		int orgIntervalCount=intervalCount;
		intervalCount=0;
		return orgIntervalCount;
	}
}
