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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.utils.Utils;

final class Closeables {

    private Closeables() {
    }

    /**
     * Closes each of the closeables, though one before it fails, and throws the first failure.
     * Every failure is logged.
     */
    static void closeAll(String name, Closeable... closeables) throws IOException {
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Utils.closeAllQuietly(failure, name, closeables);
        Throwable t = failure.get();
        if (t instanceof IOException) {
            throw (IOException) t;
        } else if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else if (t != null) {
            // A checked exception thrown sneakily, as Kotlin or Lombok code can.
            throw new IOException(t);
        }
    }
}
