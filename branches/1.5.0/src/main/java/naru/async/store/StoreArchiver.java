package naru.async.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.log4j.Logger;

/**
 * Storeの内容を以下の形式のzipファイルに出力（入力）するユーティリティ
 * +storeid1（出力時は、storeId,入力時には、名称不問)
 *  store(名称固定)
 * +storeid2（出力時は、storeId,入力時には、名称不問)
 *  store(名称固定)
 */
public class StoreArchiver{
	private static final String DEFAULT_STORE_NAME="store";
	private static Logger logger=Logger.getLogger(StoreArchiver.class);
	
	public static void toArchive(long storeId,File archive) throws IOException{
		toArchive(storeId,archive,DEFAULT_STORE_NAME);
	}
	
	public static void toArchive(long[] idsArray,File archive) throws IOException{
		toArchive(idsArray,archive,DEFAULT_STORE_NAME);
	}
	
	public static void toArchive(long[] idsArray,File archive,String name) throws IOException{
		Set<Long> storeIds=new HashSet<Long>();
		for(long storeId:idsArray){
			storeIds.add(storeId);
		}
		toArchive(storeIds,archive,name);
	}
	
	public static void toArchive(Set<Long> storeIds,File archive) throws IOException{
		toArchive(storeIds,archive,DEFAULT_STORE_NAME);
	}
	
	public static void toArchive(long storeId,File archive,String name) throws IOException{
		Set<Long> storeIds=new HashSet<Long>();
		storeIds.add(storeId);
		toArchive(storeIds,archive,name);
	}
	
	public static void toArchive(Set<Long> storeIds,File archive,String name) throws IOException{
		if(storeIds==null || storeIds.size()==0){
			logger.warn("storeIds is empty.archive:"+archive);
			return;
		}
		ZipOutputStream zos=new ZipOutputStream(new FileOutputStream(archive));
		for(Long storeId:storeIds){
			long length=StoreManager.getStoreLength(storeId);
			if(length<0){
				logger.warn("illegal storeId:"+storeId);
				continue;
			}
			ZipEntry ze=new ZipEntry(Long.toString(storeId)+"/"+name);
			ze.setSize(length);
			zos.putNextEntry(ze);
			StoreStream.storeToStream(storeId, zos);
			zos.closeEntry();
		}
		zos.close();//必要
	}
	
	//nameと後方一致するファイルだけを対象とする。
	public static Map<String,String> fromArchive(File archive) throws IOException{
		return fromArchive(archive,DEFAULT_STORE_NAME);
	}
	
	public static Map<String,String> fromArchive(File archive,String name) throws IOException{
		ZipInputStream zis=null;
		Map<String,String> result=new HashMap<String,String>();
		try{
			zis=new ZipInputStream(new FileInputStream(archive));
			while(true){
				ZipEntry ze=zis.getNextEntry();
				if(ze==null){
					break;
				}
				String fileName=ze.getName();
				if(!fileName.endsWith(name)){
					continue;
				}
				String digest=StoreStream.streamToStore(zis);
				result.put(fileName,digest);
			}
		}finally{
			if(zis!=null){
				try {
					zis.close();
				} catch (IOException ignore) {
				}
			}
		}
		return result;
	}
}
