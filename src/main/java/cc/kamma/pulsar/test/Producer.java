package cc.kamma.pulsar.test;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.StringUtils;
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

import java.net.InetAddress;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Component
public class Producer implements ApplicationRunner {

    private Logger logger = LoggerFactory.getLogger(Producer.class);
    @Value("${kafka.topic:test}")
    private String topic;

    @Value("${kafka.bytes:50}")
    private int bytes;

    @Value("${kafka.servers}")
    private String kafkaServers;

    @Value("${kafka.tps:20000}")
    private int tps;
    @Value("${kafka.batchSize:1}")
    private int batchSize;
    @Value("${kafka.threadNum:1}")
    private int threadNumber;
    @Value("${kafka.producer.batchSize:4096}")
    private int producerBatchSize;
    @Value("${kafka.acks:1}")
    private String acks;
    @Value("${kafka.par:0}")
    private int par;

    @Autowired
    private MeterRegistry meterRegistry;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Counter counter = meterRegistry.counter("producer.messages");
        Counter ack = meterRegistry.counter("producer.ack");
        Timer timer = Timer.builder("producer.ack.timer")
                .publishPercentileHistogram()
                .publishPercentiles(0.5, 0.95, 0.99)
                .sla(Duration.ofMillis(5), Duration.ofMillis(10), Duration.ofMillis(20), Duration.ofMillis(50), Duration.ofMillis(100))
                .register(meterRegistry);

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("kafkaServers")
                .build();

        org.apache.pulsar.client.api.Producer<byte[]> producer = client.newProducer()
                .topic(topic)
                .create();


        // 等待消费者
        Thread.sleep(10000);


        String hostName = InetAddress.getLocalHost().getHostName();
        byte[] body = StringUtils.repeat('x', bytes).getBytes();
        for (int j = 0; j < threadNumber; j++) {
            final int a = j;
            new Thread(() -> {
                Properties config = new Properties();

                for (; ; ) {
                    for (int i = 0; i < batchSize; i++) {
                        long time = System.nanoTime();
                        MessageId send = null;
                        try {
                            send = producer.newMessage()
                                    .key(String.valueOf(time))
                                    .value(body)
                                    .send();
                        } catch (PulsarClientException e) {
                            e.printStackTrace();
                        }
                        ack.increment();
                        long latency = System.nanoTime() - time;
                        timer.record(latency, TimeUnit.NANOSECONDS);
                        counter.increment();
                    }
//                    LockSupport.parkNanos(1000000000 / tps);
                }
            }).start();
        }


    }

}
