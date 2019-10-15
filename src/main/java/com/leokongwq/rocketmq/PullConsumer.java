package com.leokongwq.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_TEST_CONSUMER_GROUP;
import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_TEST_TOPIC;

/**
 * @author kongwenqiang
 */
public class PullConsumer {
    private static final Map<MessageQueue, Long> OFFSE_TABLE = new HashMap<MessageQueue, Long>();

    public static void main(String[] args) throws Exception {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(RMQ_TEST_CONSUMER_GROUP);
        consumer.start();
        //获取订阅topic的消费队列
        Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(RMQ_TEST_TOPIC);
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the queue: " + mq + "%n");
            SINGLE_MQ:
            while (true) {
                // 拉去消息
                PullResult pullResult = consumer.pullBlockIfNotFound(mq, null, getMessageQueueOffset(mq), 32);
                System.out.printf("%s%n", pullResult);
                putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        List<MessageExt> messageExtList = pullResult.getMsgFoundList();
                        for (MessageExt messageExt : messageExtList) {
                            System.out.println(messageExt.getMsgId());
                        }
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                        break SINGLE_MQ;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
            }
        }
        consumer.shutdown();
    }

    private static long getMessageQueueOffset(MessageQueue mq) {
        Long offset = OFFSE_TABLE.get(mq);
        if (offset != null) return offset;
        return 0;
    }
    private static void putMessageQueueOffset(MessageQueue mq, long offset) {
        OFFSE_TABLE.put(mq, offset);
    }
}
