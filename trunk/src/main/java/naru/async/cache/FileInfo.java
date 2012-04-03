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
	
	/* 統計情報 */
	private long cacheInTime;
	private long lastTime;
	private long totalCount;
	private long intervalCount;
	
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
	public long getTotalCount() {
		return totalCount;
	}
	public long getIntervalCount() {
		return intervalCount;
	}
	
	public boolean isChange(){
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

	private float lastScore=0.0f;
	
	/* 当該infoの不要度(大きいと捨てられる可能性が高い) */
	public float getScore(long now){
		float result=0.0f;
		long orgIntervalCount=intervalCount;
		intervalCount=0;
		if(orgIntervalCount>=1){
			return result;
		}
		if(totalCount==0){
			totalCount=1;
		}
		//TODO 精査する事
		return (float)(now-(long)lastTime)/(float)totalCount;
	}
	
	public float getLastScore(){
		return lastScore;
	}
}
