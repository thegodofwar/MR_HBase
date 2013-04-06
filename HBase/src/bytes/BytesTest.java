package bytes;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.util.Bytes;

public class BytesTest {
   
	public static void main(String args[]) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(8);
		byteBuffer.putInt(1234);
		byteBuffer.putInt(5678);
		byte[] rowkey = byteBuffer.array();
		/*for (byte b : rowkey) {
			System.out.println(b);
		}*/
		String sb = Bytes.toStringBinary(rowkey);
		String s = rowkey.toString();
		rowkey = s.getBytes();
		for (byte b : rowkey) {
			System.out.println(b);
		}
		System.out.println(sb);

		rowkey = "test".getBytes();
		System.out.println(Bytes.toString(rowkey));
		System.out.println(Bytes.toStringBinary(rowkey)); 
	}
	
}
