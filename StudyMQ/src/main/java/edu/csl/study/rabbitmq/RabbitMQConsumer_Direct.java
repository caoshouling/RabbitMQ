package edu.csl.study.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RabbitMQConsumer_Direct {
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

     //创建与RabbitMQ服务的TCP连接
     connection = connectionFactory.newConnection();
     //创建与Exchange的通道，每个连接可以创建多个通道，每个通道代表一个会话任务
     channel  = connection.createChannel();
     channel.basicQos(0,1,false);
     final Channel finalChannel = channel;
     Consumer consumer = new DefaultConsumer(finalChannel){
       @Override
       public void handleDelivery(String consumerTag,Envelope envelope,AMQP.BasicProperties properties,byte[] body)
               throws IOException{
         System.out.println(consumerTag+"-接受到消息："+new String(body));
         try {
           TimeUnit.MICROSECONDS.sleep(500);
         } catch (InterruptedException e) {
           e.printStackTrace();
         }
         finalChannel.basicAck(envelope.getDeliveryTag(),false);

       }
     };
     /**
      * 重要：手动确认，而不是自动确认。
      */
     boolean  autoAck  = false;
     /**
      * 消费者的概念其实是：客户端 + 队列 ，即虽然是通一个客户端连接，但是对不同的队列，服务器分配的消费者ID是不同的。
      * 每个Channel有自己的线程，最好是每个Channel对应一个消费者（即消费一个队列）。
      * 目前这种消费QUEUE_email和QUEUE_sms两个队列的做法，其实在高并发场景中会有性能问题。
      *
      */

     channel.basicConsume(QUEUE_email,autoAck,consumer);
     channel.basicConsume(QUEUE_sms,autoAck,consumer);
     System.out.println("----完成basicConsume---");
     /**
      * 注意：这里不要关闭连接，不然就退出监听了。
      */


   }
}
