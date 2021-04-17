package edu.csl.study.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class RabbitMQProductor_Batch {
   private  static final String EXCHANGE_NAME = "exchange_demo";
   private static final String ROUTING_email="routingKey_email";
   private static final String ROUTING_sms="routingKey_sms";
   private static  final String QUEUE_email= "queue_email";
  private static  final String QUEUE_sms = "queue_sms";
   private static final String IP_ADDRESS = "192.168.100.20";
   private static final int port = 5672; //服务端口默认5672
   public static void main(String[] args) throws Exception {
     ConnectionFactory connectionFactory = new ConnectionFactory();
     connectionFactory.setHost(IP_ADDRESS);//mq服务ip地址
     connectionFactory.setPort(port);//mq client连接端口
     connectionFactory.setUsername("root");//mq登录用户名
     connectionFactory.setPassword("admin");//mq登录密码
     connectionFactory.setVirtualHost("/study");//rabbitmq默认虚拟机名称为“/”，虚拟机相当于一个独立的mq服务器
     Connection connection = null;
     Channel channel  = null;
     try{
       //创建与RabbitMQ服务的TCP连接
       connection = connectionFactory.newConnection();
       //创建与Exchange的通道，每个连接可以创建多个通道，每个通道代表一个会话任务
       channel  = connection.createChannel();
       //通道绑定交换机
       channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT,true,false,false,null);
       //通道绑定队列
       //注意：消费者和生产者都可以使用queueDeclare申明一个队列。但是如果消费者在同一个信道上订阅了另一个队列，
       // 就无法再申明队列了。必须先取消订阅，然后将信道置为“传输”模式，之后才能申明队列。
       channel.queueDeclare(QUEUE_sms,true,false,false,null);//通道绑定短信队列
       //交换机和队列绑定
       channel.queueBind(QUEUE_sms,EXCHANGE_NAME,ROUTING_sms);

       SortedSet<Long> confirmSet = Collections.synchronizedSortedSet(new TreeSet<Long>());
       //指定我们的消息投递模式：消息确认模式
       channel.confirmSelect();
       channel.addConfirmListener(new ConfirmListener() {
         public void handleAck(long deliveryTag, boolean multiple) throws IOException {
           System.out.println("ack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
           if (multiple) {
             confirmSet.headSet(deliveryTag + 1).clear();
           } else {
             confirmSet.remove(deliveryTag);
           }
         }
         public void handleNack(long deliveryTag, boolean multiple) throws IOException {
           System.out.println("Nack, SeqNo: " + deliveryTag + ", multiple: " + multiple);
           if (multiple) {
             confirmSet.headSet(deliveryTag + 1).clear();
           } else {
             confirmSet.remove(deliveryTag);
           }
         }
       });
       String message_sms = new String("sms短信消息。。。");
       while (true) {
         long nextSeqNo = channel.getNextPublishSeqNo();
         channel.basicPublish(EXCHANGE_NAME, ROUTING_sms, MessageProperties.PERSISTENT_TEXT_PLAIN, message_sms.getBytes());
         confirmSet.add(nextSeqNo);
         System.out.println("发送消息："+nextSeqNo);
       }
     }finally {
//        if(channel != null){
//          channel.close();
//        }
//        if(connection != null){
//          connection.close();
//        }
     }


   }
}
