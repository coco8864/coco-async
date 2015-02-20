package naru.async.store4;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Map;

public class FileManager {
	private static FileManager instance=new FileManager();
	public static FileManager getInstance(){
		return instance;
	}
	
	private Integer fileIdCounter;
	private Map<File,StoreFile> storeFiles;
	private Map<Integer,StoreFile> idStoreFiles;
	private List<StoreFile> stackStores;
	
	private static class StoreFile{
		private Integer fileId;
		private Integer firstPageId; 
		private File file;
		private List<FileChannel> readChannels;
		private FileChannel writeChannel;
		
		StoreFile(File file,int readCount,boolean isWrite) throws FileNotFoundException{
			if(isWrite){
				writeChannel=(new FileOutputStream(file)).getChannel();
			}
			for(int i=0;i<readCount;i++){
				FileChannel readChannel=(new FileInputStream(file)).getChannel();
				readChannels.add(readChannel);
			}
			this.file=file;
			this.fileId=xxx;
		}
		
		void read(Page page){
		}
		
		void write(Page page){
		}
	}
	
	public Store open(File file){
		StoreFile storeFile=storeFiles.get(file);
		if(storeFile==null){
			storeFile=new StoreFile(file,1,false);
		}
		
	}
	
	void read(Page page){
		StoreFile storeFile=idStoreFiles.get(key);
		storeFile.read(page);
	}
	
	void write(Page page){
		StoreFile storeFile=idStoreFiles.get(key);
		storeFile.write(page);
	}
}
