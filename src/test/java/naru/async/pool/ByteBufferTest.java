package naru.async.pool;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.junit.Test;

public class ByteBufferTest {

	@Test
	public void test() {
		ByteBuffer buf=ByteBuffer.allocate(1024);
		for(int i=0;i<1025;i++){
			buf.put((byte)'a');
			System.out.println("" +i  +":"+ buf.hasRemaining());
			if(buf.hasRemaining()==false){
				break;
			}
		}
		buf.flip();
		for(int i=0;i<1025;i++){
			System.out.println("" +i +":"+ buf.get());
			if(buf.hasRemaining()==false){
				break;
			}
		}
		
		
		
	}

}
