package naru.async.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class FileChannelTest {
	private static class Reader implements Runnable{
		FileChannel readChannel;
		Reader(FileChannel hannereadChannell){
			this.readChannel=hannereadChannell;
		}
		public void run() {
			try {
				while(true){
					readChannel.position(0);
					ByteBuffer buf=ByteBuffer.allocate(10240);
					System.out.println("read"+readChannel.read(buf));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			
		}
	}
	private static class Writer implements Runnable{
		FileChannel writeChannel;
		Writer(FileChannel writeChannel){
			this.writeChannel=writeChannel;
		}
		
		public void run() {
			while(true){
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
						buf.duplicate()};
				try {
					System.out.println("write"+writeChannel.write(buf));
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
		}
	}
	

	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		RandomAccessFile raf=new RandomAccessFile("test","rwd");
		FileChannel writeChannel=raf.getChannel();

		RandomAccessFile raf1=new RandomAccessFile("test","rwd");
		FileChannel writeChannel1=raf1.getChannel();
		
		RandomAccessFile rafrd=new RandomAccessFile("test","r");
		FileChannel readChannel=rafrd.getChannel();
		RandomAccessFile rafrd1=new RandomAccessFile("test","r");
		FileChannel readChannel1=rafrd1.getChannel();
		RandomAccessFile rafrd2=new RandomAccessFile("test","r");
		FileChannel readChannel2=rafrd2.getChannel();
		RandomAccessFile rafrd3=new RandomAccessFile("test","r");
		FileChannel readChannel3=rafrd3.getChannel();
		Thread t2=new Thread(new Writer(writeChannel));
		Thread t21=new Thread(new Writer(writeChannel1));
		Thread t1=new Thread(new Reader(readChannel));
		Thread t11=new Thread(new Reader(readChannel1));
		Thread t12=new Thread(new Reader(readChannel2));
		Thread t13=new Thread(new Reader(readChannel3));
		
		t2.start();
		t21.start();
		t1.start();
		t11.start();
		t12.start();
		t13.start();

	}

}
