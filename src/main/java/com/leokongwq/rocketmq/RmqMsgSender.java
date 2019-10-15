package com.leokongwq.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;

import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_NAME_SERVER;
import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_TEST_PRODUCER_GROUP;
import static com.leokongwq.rocketmq.RmqConfigConstants.RMQ_TEST_TOPIC;

/**
 * @author kongwenqiang
 */
public class RmqMsgSender {

    public static void main(String[] args) throws Exception {

        DefaultMQProducer defaultMQProducer = new DefaultMQProducer(RMQ_TEST_PRODUCER_GROUP, new RPCHook () {
            public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
                System.out.println("before request remote broker address: " + remoteAddr);
            }

            public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
                System.out.println("after request remote broker address: " + remoteAddr);
            }
        });
        defaultMQProducer.setNamesrvAddr(RMQ_NAME_SERVER);
        defaultMQProducer.start();

        // normal send
        Message message = buildMessage("Hello RMQ!");
        SendResult sendResult = defaultMQProducer.send(message, 30000);
        System.out.println("sync send message success. messageId -> " + sendResult.getMsgId());

        // async send
        defaultMQProducer.send(message, new SendCallback() {
            public void onSuccess(SendResult sendResult) {
                System.out.println("async send success. messageId : " + sendResult.getMsgId());
            }
            public void onException(Throwable e) {
                System.out.println("async send fail. error : " + e.getMessage());
            }
        });
        // onWay
        defaultMQProducer.sendOneway(message);

        // send delayed message
        Message delayMsg = buildMessage("hello delayed message");
        delayMsg.setDelayTimeLevel(3);
        defaultMQProducer.send(delayMsg);

        // send message orderly
        for (int i = 1; i <= 10; i++) {
            SendResult sr = defaultMQProducer.send(buildMessage("message-" + i), new MessageQueueSelector() {
                public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                    Integer id = (Integer) arg;
                    int index = id % mqs.size();
                    return mqs.get(index);
                }
            }, i);
            System.out.println(sr.getMsgId());
        }

        defaultMQProducer.shutdown();
    }

    private static Message buildMessage(String body) {
        Message message = new Message(RMQ_TEST_TOPIC, body.getBytes());
        message.setKeys("key-test");
        message.setTags("tag-test-a");
        return message;
    }
}
