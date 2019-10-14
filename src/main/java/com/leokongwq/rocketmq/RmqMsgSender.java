package com.leokongwq.rocketmq;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

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

        Message message = new Message(RMQ_TEST_TOPIC, "hello".getBytes());
        message.setKeys("key-test");
        message.setTags("tag-test-a");
        SendResult sendResult = defaultMQProducer.send(message, 30000);

        System.out.println("messageId -> " + sendResult.getMsgId());

        defaultMQProducer.shutdown();
    }
}
