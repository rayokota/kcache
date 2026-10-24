/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.kcache;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.kcache.utils.ClusterTestHarness;
import io.kcache.utils.StringUpdateHandler;
import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import javax.management.ObjectName;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class KafkaCacheInitFailureTest extends ClusterTestHarness {

    // An init that fails once the reader thread exists: the topic has one partition, not three.
    @Test
    public void testCloseAfterAFailedInitReturns() throws Exception {
        Properties props = properties("failed-read");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_PARTITIONS_CONFIG, "0,1,2");
        KafkaCache<String, String> cache = cache(props);
        failInit(cache);
        FutureTask<Void> close = new FutureTask<>(() -> {
            cache.close();
            return null;
        });
        Thread closer = new Thread(close);
        closer.setDaemon(true);
        closer.start();
        close.get(30, TimeUnit.SECONDS);
    }

    // An init that fails as the reader thread is made, since the topic does not exist.
    @Test
    public void testAFailedInitLeavesNoConsumer() throws Exception {
        Properties props = properties("failed-subscribe");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, "missing");
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_SKIP_VALIDATION_CONFIG, true);
        props.put("kafkacache." + ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        KafkaCache<String, String> cache = cache(props);
        failInit(cache);
        assertTrue("The consumer is closed", ManagementFactory.getPlatformMBeanServer()
            .queryNames(new ObjectName("kafka.consumer:type=app-info,id=failed-subscribe"), null)
            .isEmpty());
        cache.close();
    }

    private Properties properties(String clientId) {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_BACKING_CACHE_CONFIG, CacheType.MEMORY.toString());
        props.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
        props.put(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, 2000);
        return props;
    }

    private static KafkaCache<String, String> cache(Properties props) {
        return new KafkaCache<>(new KafkaCacheConfig(props), Serdes.String(), Serdes.String(),
            new StringUpdateHandler(), null);
    }

    private static void failInit(KafkaCache<String, String> cache) {
        try {
            cache.init();
            fail("init should fail");
        } catch (RuntimeException expected) {
            // a CacheInitializationException, or the consumer's own failure
        }
    }
}
