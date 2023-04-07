/*
 * DaRPC: Data Center Remote Procedure Call
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016-2018, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package de.invesdwin.context.integration.channel.sync.darpc;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.darpc.DaRPCMessage;

import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class RdmaRpcMessage implements DaRPCMessage {

    private final IByteBuffer message;
    private int size;

    public RdmaRpcMessage(final int socketSize) {
        this.message = ByteBuffers.allocateDirect(socketSize);
    }

    public IByteBuffer getMessage() {
        return message;
    }

    public int getSize() {
        return size;
    }

    public void setSize(final int size) {
        this.size = size;
    }

    @Override
    public int size() {
        return message.capacity();
    }

    @Override
    public void update(final java.nio.ByteBuffer buffer) {
        message.putBytes(0, buffer, buffer.position(), buffer.remaining());
        size = buffer.remaining();
        ByteBuffers.position(buffer, buffer.limit());
    }

    @Override
    public int write(final java.nio.ByteBuffer buffer) {
        final int length = buffer.remaining();
        message.getBytes(0, buffer);
        ByteBuffers.position(buffer, buffer.limit());
        return length;
    }

}