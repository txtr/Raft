package io.hamster.storage.journal;

import java.nio.ByteBuffer;

public class ByteBuffTest {

    public static void main(String[] args) {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        printBuffInfo(buffer);

        buffer.mark();
        buffer.putInt(100);
        buffer.putInt(200);
        printBuffInfo(buffer);
        //buffer.reset();
        buffer.position(0);
        buffer.limit(8);
        printBuffInfo(buffer);
        System.out.println(buffer.getInt());
        printBuffInfo(buffer);
        System.out.println(buffer.getInt());
        printBuffInfo(buffer);
    }

    private static void printBuffInfo(ByteBuffer byteBuffer){
        String s = "pos:" + byteBuffer.position() +
                 " , limit:" + byteBuffer.limit() +
                ", cap:" + byteBuffer.capacity() ;
        System.out.println(s);
    }
}
