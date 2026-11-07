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

import io.kcache.exceptions.CacheException;
import io.kcache.utils.ClusterTestHarness;
import io.kcache.utils.StringUpdateHandler;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Test;

public class KafkaCacheSyncFailureTest extends ClusterTestHarness {

    // Deleted topics stay deleted, rather than coming back empty on the reader's next fetch.
    @Override
    protected void injectProperties(Properties props) {
        super.injectProperties(props);
        props.setProperty("auto.create.topics.enable", "false");
        props.setProperty("delete.topic.enable", "true");
    }

    // A sync on a deleted topic cannot read to its end: it fails, rather than retry for good.
    @Test
    public void testASyncOfADeletedTopicFails() throws Exception {
        String topic = "deleted-before-sync";
        Properties writing = properties("deleted-before-sync-writer", topic);
        try (KafkaCache<String, String> writer = cache(writing)) {
            writer.init();
            writer.put("k", "v");
        }
        // A reader that wrote nothing syncs by reading to the topic's end offsets.
        KafkaCache<String, String> reader = cache(properties("deleted-before-sync", topic));
        reader.init();
        try (Admin admin = Admin.create(Collections.singletonMap(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            admin.deleteTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
        }
        FutureTask<Void> sync = new FutureTask<>(() -> {
            reader.sync();
            return null;
        });
        Thread syncer = new Thread(sync);
        syncer.setDaemon(true);
        syncer.start();
        try {
            sync.get(60, TimeUnit.SECONDS);
            fail("sync should fail");
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof CacheException);
        }
        reader.close();
    }

    private Properties properties(String clientId, String topic) {
        Properties props = new Properties();
        props.put(KafkaCacheConfig.KAFKACACHE_BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(KafkaCacheConfig.KAFKACACHE_CLIENT_ID_CONFIG, clientId);
        props.put(KafkaCacheConfig.KAFKACACHE_TOPIC_CONFIG, topic);
        props.put(KafkaCacheConfig.KAFKACACHE_INIT_TIMEOUT_CONFIG, 2000);
        props.put(KafkaCacheConfig.KAFKACACHE_TIMEOUT_CONFIG, 2000);
        return props;
    }

    private static KafkaCache<String, String> cache(Properties props) {
        return new KafkaCache<>(new KafkaCacheConfig(props), Serdes.String(), Serdes.String(),
            new StringUpdateHandler(), null);
    }
}
