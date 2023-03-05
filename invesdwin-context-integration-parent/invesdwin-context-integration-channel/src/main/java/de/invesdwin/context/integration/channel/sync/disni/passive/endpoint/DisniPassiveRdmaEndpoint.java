package de.invesdwin.context.integration.channel.sync.disni.passive.endpoint;

import java.io.IOException;
import java.util.LinkedList;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvRecvWR;
import com.ibm.disni.verbs.IbvSendWR;
import com.ibm.disni.verbs.IbvSge;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPollCq;

@NotThreadSafe
public class DisniPassiveRdmaEndpoint extends RdmaEndpoint {

    private final int socketSize;

    private java.nio.ByteBuffer sendBuf;
    private IbvMr sendMr;
    private java.nio.ByteBuffer recvBuf;
    private IbvMr recvMr;

    private final LinkedList<IbvSendWR> wrList_send;
    private final IbvSge sgeSend;
    private final LinkedList<IbvSge> sgeList;
    private final IbvSendWR sendWR;

    private final LinkedList<IbvRecvWR> wrList_recv;
    private final IbvSge sgeRecv;
    private final LinkedList<IbvSge> sgeListRecv;
    private final IbvRecvWR recvWR;

    private IbvWC[] wcList;

    private SVCPollCq poll;

    public DisniPassiveRdmaEndpoint(final RdmaEndpointGroup<DisniPassiveRdmaEndpoint> endpointGroup,
            final RdmaCmId idPriv, final boolean serverSide, final int socketSize) throws IOException {
        super(endpointGroup, idPriv, serverSide);

        this.socketSize = socketSize;

        this.wrList_send = new LinkedList<IbvSendWR>();
        this.sgeSend = new IbvSge();
        this.sgeList = new LinkedList<IbvSge>();
        this.sendWR = new IbvSendWR();

        this.wrList_recv = new LinkedList<IbvRecvWR>();
        this.sgeRecv = new IbvSge();
        this.sgeListRecv = new LinkedList<IbvSge>();
        this.recvWR = new IbvRecvWR();
    }

    //important: we override the init method to prepare some buffers (memory registration, post recv, etc).
    //This guarantees that at least one recv operation will be posted at the moment this endpoint is connected.
    @Override
    public void init() throws IOException {
        super.init();

        final IbvCQ cq = getCqProvider().getCQ();
        //        Assertions.checkEquals(1, channel.getEndpoint().getCqProvider().getCqSize());
        this.wcList = new IbvWC[getCqProvider().getCqSize()];
        for (int i = 0; i < wcList.length; i++) {
            wcList[i] = new IbvWC();
        }
        this.poll = cq.poll(wcList, wcList.length);

        this.sendBuf = java.nio.ByteBuffer.allocateDirect(socketSize);
        this.sendMr = registerMemory(sendBuf).execute().free().getMr();
        this.recvBuf = java.nio.ByteBuffer.allocateDirect(socketSize);
        this.recvMr = registerMemory(recvBuf).execute().free().getMr();

        sgeSend.setAddr(sendMr.getAddr());
        sgeSend.setLength(sendMr.getLength());
        sgeSend.setLkey(sendMr.getLkey());
        sgeList.add(sgeSend);
        sendWR.setWr_id(2000);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        wrList_send.add(sendWR);

        sgeRecv.setAddr(recvMr.getAddr());
        sgeRecv.setLength(recvMr.getLength());
        final int lkey = recvMr.getLkey();
        sgeRecv.setLkey(lkey);
        sgeListRecv.add(sgeRecv);
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(2001);
        wrList_recv.add(recvWR);

        this.postRecv(wrList_recv).execute().free();
    }

    public LinkedList<IbvSendWR> getWrList_send() {
        return wrList_send;
    }

    public LinkedList<IbvRecvWR> getWrList_recv() {
        return wrList_recv;
    }

    public java.nio.ByteBuffer getSendBuf() {
        return sendBuf;
    }

    public java.nio.ByteBuffer getRecvBuf() {
        return recvBuf;
    }

    public IbvSendWR getSendWR() {
        return sendWR;
    }

    public IbvRecvWR getRecvWR() {
        return recvWR;
    }

    public SVCPollCq getPoll() {
        return poll;
    }

    @Override
    public synchronized void close() throws IOException, InterruptedException {
        poll.free();
        poll = null;
        deregisterMemory(recvMr);
        recvMr = null;
        recvBuf = null;
        deregisterMemory(sendMr);
        sendMr = null;
        sendBuf = null;
        super.close();
    }

}
