package com.hmdp.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ConfirmedRabbitPublisherTest {

    private RabbitTemplate rabbitTemplate;
    private ConfirmedRabbitPublisher publisher;

    @BeforeEach
    void setUp() {
        rabbitTemplate = mock(RabbitTemplate.class);
        publisher = new ConfirmedRabbitPublisher();
        ReflectionTestUtils.setField(publisher, "rabbitTemplate", rabbitTemplate);
    }

    @Test
    void acceptsConfirmedRoutableMessage() {
        completePublish(true, false);
        assertDoesNotThrow(() -> publisher.send("exchange", "key", "payload", "id-1"));
    }

    @Test
    void rejectsBrokerNack() {
        completePublish(false, false);
        assertThrows(IllegalStateException.class,
                () -> publisher.send("exchange", "key", "payload", "id-2"));
    }

    @Test
    void rejectsUnroutableMessage() {
        completePublish(true, true);
        assertThrows(IllegalStateException.class,
                () -> publisher.send("exchange", "key", "payload", "id-3"));
    }

    private void completePublish(boolean ack, boolean returned) {
        ArgumentCaptor<CorrelationData> captor = ArgumentCaptor.forClass(CorrelationData.class);
        doAnswer(invocation -> {
            // 第 4 位是 CorrelationData：发送路径现在统一走带 MessagePostProcessor 的重载
            // （traceId 就是靠那个后置处理器塞进消息头的），
            // 所以打桩必须打这个签名。打错重载的话 future 永远不会完成，
            // 三个用例会全部变成"等 5 秒超时"才失败——看着像通过，其实什么都没测。
            CorrelationData correlationData = invocation.getArgument(4);
            if (returned) {
                correlationData.setReturnedMessage(
                        new Message(new byte[0], new MessageProperties()));
            }
            correlationData.getFuture().set(new CorrelationData.Confirm(ack, ack ? null : "nack"));
            return null;
        }).when(rabbitTemplate).convertAndSend(
                eq("exchange"), eq("key"), eq("payload"), any(MessagePostProcessor.class), captor.capture());
    }

    @Test
    void stampsCurrentTraceIdIntoMessageHeader() throws Exception {
        completePublish(true, false);
        try (TraceContext.Scope ignored = TraceContext.enter("publisher-test-01")) {
            publisher.send("exchange", "key", "payload", "id-4");
        }
        assertEquals("publisher-test-01", postedHeaders().get(TraceContext.HEADER));
    }

    @Test
    void leavesHeaderOutWhenNoTraceInCurrentThread() throws Exception {
        completePublish(true, false);
        publisher.send("exchange", "key", "payload", "id-5");
        assertFalse(postedHeaders().containsKey(TraceContext.HEADER),
                "后台线程自发的消息没有上游，塞空值只会让消费端误以为接上了");
    }

    @Test
    void keepsCallerPostProcessorWhileStampingTrace() throws Exception {
        completePublish(true, false);
        try (TraceContext.Scope ignored = TraceContext.enter("publisher-test-02")) {
            publisher.send("exchange", "key", "payload",
                    message -> {
                        message.getMessageProperties().setExpiration("1000");
                        return message;
                    }, "id-6");
        }
        MessageProperties properties = postedProperties();
        assertEquals("1000", properties.getExpiration(), "包装后置处理器不能把原来的 TTL 弄丢");
        assertEquals("publisher-test-02", properties.getHeaders().get(TraceContext.HEADER));
    }

    private MessageProperties postedProperties() throws Exception {
        ArgumentCaptor<MessagePostProcessor> captor = ArgumentCaptor.forClass(MessagePostProcessor.class);
        verify(rabbitTemplate).convertAndSend(
                eq("exchange"), eq("key"), eq("payload"), captor.capture(), any(CorrelationData.class));
        return captor.getValue()
                .postProcessMessage(new Message(new byte[0], new MessageProperties()))
                .getMessageProperties();
    }

    private Map<String, Object> postedHeaders() throws Exception {
        return postedProperties().getHeaders();
    }
}
