package com.leokongwq.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_NAME_SERVER;
import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_TEST_CONSUMER_GROUP;
import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_TEST_TOPIC;

/**
 * @author kongwenqiang
 */
public class ConcurrentlyPushConsumer implements MessageListenerConcurrently {

    public static void main(String[] args) throws Exception {
        ConcurrentlyPushConsumer concurrentlyConsumer = new ConcurrentlyPushConsumer();

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RMQ_TEST_CONSUMER_GROUP);
        consumer.setNamesrvAddr(RMQ_NAME_SERVER);
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.subscribe(RMQ_TEST_TOPIC, "");

        //consumer.setMessageModel(MessageModel.BROADCASTING);

        consumer.registerMessageListener(concurrentlyConsumer);
        consumer.setPullBatchSize(300);
        consumer.setConsumeMessageBatchMaxSize(30);
        consumer.setConsumeThreadMin(20);
        consumer.setConsumeThreadMax(200);
        consumer.start();

        Thread.sleep(1000 * 30000);
    }

    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt messageExt : msgs) {
            System.out.println(new String(messageExt.getBody()));
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }
}
