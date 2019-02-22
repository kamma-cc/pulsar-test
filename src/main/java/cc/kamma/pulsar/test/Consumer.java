package cc.kamma.pulsar.test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class Consumer implements ApplicationRunner {

    private Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Value("${kafka.topic:test}")
    private String topic;

    @Value("${kafka.servers}")
    private String kafkaServers;

    @Value("${kafka.consumer.groupId:0}")
    private String groupId;

    @Autowired
    private MeterRegistry meterRegistry;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Counter counter = meterRegistry.counter("consumer.messages");
        Timer consumerTimer = Timer.builder("consumer.receive.timer")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);


        PulsarClient client = PulsarClient.builder()
                .serviceUrl("kafkaServers")
                .build();

        org.apache.pulsar.client.api.Consumer consumer = client.newConsumer()
                .topic(topic)
                .subscriptionName("my-subscription")
                .subscribe();

        consumer.seek(MessageId.latest);

        new Thread(() -> {
            while (true) {
                try {
                    Message msg = consumer.receive();
                    counter.increment();
                    long nano = System.nanoTime() - Long.valueOf(msg.getKey());
                    consumerTimer.record(nano, TimeUnit.NANOSECONDS);
                    consumer.acknowledge(msg);
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }


            }
        }).start();

    }
}
