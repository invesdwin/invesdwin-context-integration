package de.invesdwin.context.integration.channel.sync.netty;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.Immutable;

import io.netty.channel.Channel;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.TimeoutException;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

@Immutable
public class FakeChannelPromise implements ChannelPromise {

    public static final FakeChannelPromise INSTANCE = new FakeChannelPromise();

    @Override
    public boolean trySuccess(final Void result) {
        return false;
    }

    @Override
    public boolean tryFailure(final Throwable cause) {
        return false;
    }

    @Override
    public boolean setUncancellable() {
        return false;
    }

    @Override
    public boolean isDone() {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public Void get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }

    @Override
    public Void get() throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public boolean isCancellable() {
        return false;
    }

    @Override
    public Void getNow() {
        return null;
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public boolean cancel(final boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(final long timeout, final TimeUnit unit) {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(final long timeoutMillis) {
        return false;
    }

    @Override
    public boolean await(final long timeout, final TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public boolean await(final long timeoutMillis) throws InterruptedException {
        return false;
    }

    @Override
    public boolean isVoid() {
        return false;
    }

    @Override
    public ChannelPromise unvoid() {
        return null;
    }

    @Override
    public boolean trySuccess() {
        return false;
    }

    @Override
    public ChannelPromise syncUninterruptibly() {
        return null;
    }

    @Override
    public ChannelPromise sync() throws InterruptedException {
        return null;
    }

    @Override
    public ChannelPromise setSuccess(final Void result) {
        return null;
    }

    @Override
    public ChannelPromise setSuccess() {
        return null;
    }

    @Override
    public ChannelPromise setFailure(final Throwable cause) {
        return null;
    }

    @Override
    public ChannelPromise awaitUninterruptibly() {
        return null;
    }

    @Override
    public ChannelPromise await() throws InterruptedException {
        return null;
    }

    @Override
    public Channel channel() {
        return null;
    }

    @Override
    public ChannelPromise addListener(final GenericFutureListener<? extends Future<? super Void>> listener) {
        return null;
    }

    @Override
    public ChannelPromise addListeners(final GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return null;
    }

    @Override
    public ChannelPromise removeListener(final GenericFutureListener<? extends Future<? super Void>> listener) {
        return null;
    }

    @Override
    public ChannelPromise removeListeners(final GenericFutureListener<? extends Future<? super Void>>... listeners) {
        return null;
    }

}