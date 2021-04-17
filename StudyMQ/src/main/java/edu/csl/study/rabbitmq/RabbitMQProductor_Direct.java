package edu.csl.study.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RabbitMQProductor_Direct {
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
       /**
        * Declare an exchange.
        * @param exchange 交换机名称
        * @param type 交换机类型 fanout、topic、direct、headers
        * @param durable 是否持久化
        * @param autoDelete 是否自动删除。自动删除的前提是至少有一个队列或者交换机与这个交换机绑定，
        *                   之后所有与这个交换机绑定的队列或者交换机都与此解绑。
        *                   错误理解：与此交换机连接的客户端都端口，RabbmitMQ会自动删除本交换机
        *                   简单理解：原来有绑定的，后来没有绑定的了，就会自动删除。
        * @param internal 是否是内置的。如果是true表示内置交换机。客户端程序无法直接发送消息到这个交换机。
        *                 只能通过交换机路由到交换机这种方式
        * @param arguments 其他交换机参数
        * @return a declaration-confirm method to indicate the exchange was successfully declared
        */
       //Routing 路由模式
       //direct 持久化，非自动删除的交换机
       channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT,true,false,false,null);
       //通道绑定队列
       /**
        * 声明队列
        * @param queue 队列名称
        * @param durable 是否持久化
        * @param exclusive 是否排他。如果为排他的，那么该队列仅对首次声明它的连接可见。并在连接断开时自动删除。
        *                  排他队列是基于连接可见的，同一个连接的不同Channel可以同时访问同一连接创建的排他队列。
        *                  首次是指，如果一个连接声明了一个排他队列，其他连接是不允许建立同名的排他队列；
        *                  即使该队列是持久化的，一旦连接关闭或者客户端退出，该排他队列会自动删除。
        *                  这种队列适用于一个客户端同时发送和读取消息的应用场景
        * @param autoDelete 是否自动删除。自动删除的前提：至少有一个消费者连接到这个队列，
        *                   之后所有与这个队列连接的消费者都端口时，才会自动删除。
        *                   错误理解：当连接到这个队列的所有客户端都端开始，这个队列就自动删除。
        *                   因为生产者客户端创建这个队列或者没有消费者客户端与这个队列连接，都不会自动删除这个队列。
        *                   简单理解：原来有绑定的，后来没有绑定的了，就会自动删除。
        * @param arguments 其他参数，比如x-message-ttl、x-expires、x-max-length、x-max-length-bytes、
        *                  x-dead-letter-exchange、x-dead-letter-routing-key、x-max-priority等
        */
       //注意：消费者和生产者都可以使用queueDeclare申明一个队列。但是如果消费者在同一个信道上订阅了另一个队列，
       // 就无法再申明队列了。必须先取消订阅，然后将信道置为“传输”模式，之后才能申明队列。
       channel.queueDeclare(QUEUE_email,true,false,false,null);//通道绑定邮件队列
       channel.queueDeclare(QUEUE_sms,true,false,false,null);//通道绑定短信队列

       //交换机和队列绑定
       /**
        * 一、参数明细
        * 1、队列名称
        * 2、交换机名称
        * 3、路由key
        * 4.其他参数
        *
        * 二、也可以解绑
        *   channel.queueUnbind(String queue, String exchange, String routingKey, Map<String, Object> arguments)
        * 三、交换机与交换机也能绑定
        *   channel.exchangeBind(String destination, String source, String routingKey, Map<String, Object> arguments)
        */
       //Routing 路由模式
       channel.queueBind(QUEUE_email,EXCHANGE_NAME,ROUTING_email);
       channel.queueBind(QUEUE_sms,EXCHANGE_NAME,ROUTING_sms);

       //指定我们的消息投递模式：消息确认模式
       boolean mandatory = true; //mandatory为true时addReturnListener的监听器才有效。
       channel.confirmSelect();
       /**
        * Publish a message.
        * @param exchange tExchange的名称，如果没有指定，则使用Default Exchange
        * @param routingKey routingKey
        * @param props 其他属性
        * @param body 消息内容
        * mandatory 和 immediate默认都是false
        * basicPublish(String exchange, String routingKey, BasicProperties props, byte[] body)
        * ps：原来有个immediate参数，现在已经废弃了。
        * basicPublish(String exchange, String routingKey, boolean mandatory, BasicProperties props, byte[] body)
        *
        */
       String message_email = new String("email消息。。。");
       channel.basicPublish(EXCHANGE_NAME,ROUTING_email, mandatory,MessageProperties.PERSISTENT_TEXT_PLAIN,message_email.getBytes());
       System.out.println("email消息发送成功！");



       channel.addReturnListener(new ReturnListener() {
         @Override
         public void handleReturn(int i, String s, String s1, String s2,                   AMQP.BasicProperties basicProperties, byte[] bytes) throws IOException {
           System.out.println("------handle return------");
           System.out.println("replycode:"+i);
           System.out.println("replyText:"+s);
           System.out.println("exchange_name:"+s1);
           System.out.println("routingKey:"+s2);
           System.out.println("properties:"+basicProperties);
           System.out.println("body:"+new String(bytes,"utf-8"));
         }
       });
       //添加一个确认监听
       channel.addConfirmListener(new ConfirmListener() {
         //投递成功
         @Override
         public void handleAck(long DeliveryTag, boolean multiple) throws IOException {
           System.out.println(DeliveryTag+"-------ack!  multiple = "+multiple);
         }
         //投递失败
         @Override
         public void handleNack(long DeliveryTag, boolean multiple) throws IOException {
           System.out.println(DeliveryTag+"-------no ack!  multiple = "+multiple);
         }
       });
       while (true) {
         long nextSeqNo = channel.getNextPublishSeqNo();
         try {
           TimeUnit.MICROSECONDS.sleep(500);
         } catch (InterruptedException e) {
           e.printStackTrace();
         }
         String message_sms = new String("sms短信消息。。。");
         channel.basicPublish(EXCHANGE_NAME,ROUTING_sms, mandatory,MessageProperties.PERSISTENT_TEXT_PLAIN,message_sms.getBytes());
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
