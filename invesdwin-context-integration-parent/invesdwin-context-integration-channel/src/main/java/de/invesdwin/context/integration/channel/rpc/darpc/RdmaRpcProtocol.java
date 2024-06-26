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

package de.invesdwin.context.integration.channel.rpc.darpc;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.darpc.DaRPCProtocol;

@NotThreadSafe
public class RdmaRpcProtocol implements DaRPCProtocol<RdmaRpcMessage, RdmaRpcMessage> {

    private final int socketSize;

    public RdmaRpcProtocol(final int socketSize) {
        this.socketSize = socketSize;
    }

    @Override
    public RdmaRpcMessage createRequest() {
        return new RdmaRpcMessage(socketSize);
    }

    @Override
    public RdmaRpcMessage createResponse() {
        return new RdmaRpcMessage(socketSize);
    }
}
