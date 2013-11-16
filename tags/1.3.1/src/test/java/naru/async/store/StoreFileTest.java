package naru.async.store;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.ByteBuffer;

import naru.async.store.StoreFile;

import org.junit.Test;

public class StoreFileTest {
	@Test
	public void testStoreFile() throws Throwable{
		StoreFile storeFile=new StoreFile(new File("storeFile.test"),1);
		storeFile.write(ByteBuffer.wrap("test".getBytes()),1024);
		storeFile.close();
		
		storeFile=new StoreFile(new File("storeFile.test"),1);
		assertEquals(1028,storeFile.length());
		
		ByteBuffer buffer=ByteBuffer.allocate(128);
		storeFile.read(buffer, 1024);
		buffer.flip();
		String str=new String(buffer.array(),0,buffer.limit());
		assertEquals("test",str);
		storeFile.truncate();
		storeFile.close();
		
		storeFile=new StoreFile(new File("storeFile.test"),1);
		assertEquals(0,storeFile.length());
		storeFile.close();
		
		storeFile=new StoreFile(new File("storeFile.test"),1);
		
		for(int j=0;j<100;j++){
		ByteBuffer buf=ByteBuffer.allocate(10240);
		for(int i=0;i<10240;i++){
			buf.put((byte)'a');
		}
		buf.flip();
		ByteBuffer[]bufs=new ByteBuffer[]{
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate(),
				buf.duplicate()
				};
		storeFile.write(bufs);
		}
		storeFile.close();
		
		
		
		new File("storeFile.test").delete();
	}

}
