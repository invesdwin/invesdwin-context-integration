/*
 * JPPF. Copyright (C) 2005-2016 JPPF Team. http://www.jppf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package de.invesdwin.context.integration.jppf.internal;

import java.io.InputStream;
import java.io.OutputStream;

import javax.annotation.concurrent.Immutable;

import org.jppf.serialization.JPPFCompositeSerialization;

import de.invesdwin.context.integration.compression.lz4.LZ4Streams;

@Immutable
public class HighLZ4Serialization extends JPPFCompositeSerialization {
    @Override
    public void serialize(final Object o, final OutputStream os) throws Exception {
        try (OutputStream lz4os = LZ4Streams.newDefaultLZ4OutputStream(os)) {
            getDelegate().serialize(o, lz4os);
        }
    }

    @Override
    public Object deserialize(final InputStream is) throws Exception {
        try (InputStream lz4is = LZ4Streams.newDefaultLZ4InputStream(is)) {
            return getDelegate().deserialize(lz4is);
        }
    }

    @Override
    public String getName() {
        return "HighLZ4";
    }
}
