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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/** Whether a read to end offsets moved, as it decides when the read has stalled. */
public class KafkaCacheReadProgressTest {

    private static final TopicPartition P0 = new TopicPartition("topic", 0);
    private static final TopicPartition P1 = new TopicPartition("topic", 1);

    // A partition that has reached its end offset drops out of the later take: that is no move,
    // so that a read stalled beside it times out after one timeout rather than two.
    @Test
    public void testAPartitionDroppingOutIsNoMove() {
        assertFalse(KafkaCache.hasMoved(positions(5, 0), positions(5)));
    }

    @Test
    public void testAPositionThatGoesOnIsAMove() {
        assertTrue(KafkaCache.hasMoved(positions(5, 0), positions(6)));
        assertTrue(KafkaCache.hasMoved(positions(5, 0), positions(5, 1)));
    }

    @Test
    public void testPositionsThatStayAreNoMove() {
        assertFalse(KafkaCache.hasMoved(positions(5, 0), positions(5, 0)));
    }

    // Positions of partitions 0, 1, and so on, in turn.
    private static Map<TopicPartition, Long> positions(long... offsets) {
        Map<TopicPartition, Long> positions = new HashMap<>();
        TopicPartition[] partitions = {P0, P1};
        for (int i = 0; i < offsets.length; i++) {
            positions.put(partitions[i], offsets[i]);
        }
        return positions;
    }
}
