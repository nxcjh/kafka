package com.autohome.kafka_comsumer;

import java.util.concurrent.Callable;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
 
public class ConsumerTest implements Runnable {
    private KafkaStream m_stream;
    private int m_threadNumber;
 
    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        System.out.println("new ConsumerTest"+a_threadNumber+"::");
    }
 
    public void run() {
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        System.out.println("run :"+it.clientId()+ it.length());
        while (it.hasNext()){
        	System.out.println(it.next());
        	System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
        }
        	System.out.println("Shutting down Thread: " + m_threadNumber);
    }
}
