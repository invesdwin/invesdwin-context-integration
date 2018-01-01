package de.invesdwin.integration.jppf.internal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import javax.annotation.concurrent.Immutable;

import org.jppf.comm.interceptor.AbstractNetworkConnectionInterceptor;

import de.invesdwin.integration.jppf.JPPFClientProperties;

@Immutable
public class BasicAuthenticationNetworkConnectionInterceptor extends AbstractNetworkConnectionInterceptor {

    @Override
    public boolean onAccept(final Socket acceptedSocket) {
        try {
            System.out.println(acceptedSocket.getSoTimeout());
            final InputStream is = acceptedSocket.getInputStream();
            final OutputStream os = acceptedSocket.getOutputStream();
            final String userName = read(is);
            if (!userName.equals(JPPFClientProperties.USER_NAME)) {
                // send invalid user response
                write("invalid user name", os);
                return false;
            } else {
                // send ok response
                write("OK", os);
                return true;
            }
        } catch (final EOFException e) {
            //ignore eof, since checks for port availability will lead to this
            return true;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean onConnect(final Socket connectedSocket) {
        try {
            final InputStream is = connectedSocket.getInputStream();
            final OutputStream os = connectedSocket.getOutputStream();
            // send the user name to the server
            write(JPPFClientProperties.USER_NAME, os);
            // read the server reponse
            final String response = read(is);
            if (!"OK".equals(response)) {
                throw new IllegalStateException("Invalid response from server: " + response);
            } else {
                return true;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    static void write(final String message, final OutputStream destination) throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream cos = new DataOutputStream(baos)) {
            cos.writeUTF(message);
        }
        final DataOutputStream dos = new DataOutputStream(destination);
        final byte[] encrypted = baos.toByteArray();
        dos.writeInt(encrypted.length);
        dos.write(encrypted);
        dos.flush();
    }

    static String read(final InputStream source) throws Exception {
        final DataInputStream dis = new DataInputStream(source);
        final int len = dis.readInt();
        final byte[] message = new byte[len];
        dis.read(message);
        try (DataInputStream cis = new DataInputStream(new ByteArrayInputStream(message))) {
            return cis.readUTF();
        }
    }

}
