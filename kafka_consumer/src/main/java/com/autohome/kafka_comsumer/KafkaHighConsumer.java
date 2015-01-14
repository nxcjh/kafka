package com.autohome.kafka_comsumer;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * KafkaComsumer 高级接口
 * @author nxcjh
 *
 */
public class KafkaHighConsumer {

	private Properties prop;
	public ConsumerConfig loadConfiguration(){
		prop = new Properties();
		prop.put("zookeeper.connect","10.168.100.182:2181,10.168.100.183:2181,10.168.100.184:2181");
		prop.put("auto.commit.enable","true");
		prop.put("auto.commit.intercal.ms","60000");
		prop.put("group.id","nxcjh");
		System.out.println("load properties;");
		return new ConsumerConfig(prop);
		
	}
	
	public ConsumerConnector getConnector(){
		ConsumerConfig config = loadConfiguration();
		System.out.println("create consumer;");
		return Consumer.createJavaConsumerConnector(config);
	}
	
	
	public void read() throws InterruptedException, UnsupportedEncodingException{
		ConsumerConnector connector = getConnector();
		//topic的过滤
		Whitelist whitelist = new Whitelist("kafka-h-c");
		List<KafkaStream<byte[],byte[]>> partitions = connector.createMessageStreamsByFilter(whitelist);
		
		if(CollectionUtils.isEmpty(partitions)){
			System.out.println("empty");
			//TimeUnit.SECONDS.sleep(1);\
			Thread.sleep(6000);
		}
		System.out.println("starting read..");
		//消费消息
		for(KafkaStream<byte[],byte[]> partition : partitions){
			ConsumerIterator<byte[],byte[]> iterator = partition.iterator();
			while(iterator.hasNext()){
				MessageAndMetadata<byte[],byte[]> next = iterator.next();
				System.out.println("partition: "+ next.partition());
				System.out.println("offset: "+ next.offset());
				System.out.println("message: "+new String(next.message(), "utf-8"));
			}
		}
		
	}
	public static void main(String[] args) {
		KafkaHighConsumer consumer = new KafkaHighConsumer();
		try {
			System.out.println("starting reading.....");
			consumer.read();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
