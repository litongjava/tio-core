package com.litongjava.tio.client;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

import com.litongjava.enhance.buffer.VirtualBuffer;
import com.litongjava.tio.client.intf.ClientAioListener;
import com.litongjava.tio.core.ChannelCloseCode;
import com.litongjava.tio.core.Node;
import com.litongjava.tio.core.ReadCompletionHandler;
import com.litongjava.tio.core.Tio;
import com.litongjava.tio.core.TioConfig;
import com.litongjava.tio.core.pool.BufferPoolUtils;
import com.litongjava.tio.core.ssl.SslFacadeContext;
import com.litongjava.tio.core.ssl.SslUtils;
import com.litongjava.tio.core.stat.IpStat;
import com.litongjava.tio.utils.SystemTimer;
import com.litongjava.tio.utils.hutool.CollUtil;

import lombok.extern.slf4j.Slf4j;

/**
 *
 * @author tanyaowu
 * 2017年4月1日 上午9:32:10
 */
@Slf4j
public class ConnectionCompletionHandler implements CompletionHandler<Void, ConnectionCompletionVo> {

  @Override
  public void completed(Void result, ConnectionCompletionVo attachment) {
    handler(result, attachment, null);
  }

  @Override
  public void failed(Throwable throwable, ConnectionCompletionVo attachment) {
    handler(null, attachment, throwable);
  }


  private void handler(Void result, ConnectionCompletionVo attachment, Throwable throwable) {
    ClientChannelContext channelContext = attachment.getChannelContext();
    AsynchronousSocketChannel asynchronousSocketChannel = attachment.getAsynchronousSocketChannel();
    TioClient tioClient = attachment.getTioClient();
    ClientTioConfig clientTioConfig = tioClient.getClientTioConfig();
    Node serverNode = attachment.getServerNode();
    String bindIp = attachment.getBindIp();
    Integer bindPort = attachment.getBindPort();
    ClientAioListener clientAioListener = clientTioConfig.getClientAioListener();
    boolean isReconnect = attachment.isReconnect();
    boolean isConnected = false;

    try {
      if (throwable == null) {
        if (isReconnect) {
          channelContext.setAsynchronousSocketChannel(asynchronousSocketChannel);

          clientTioConfig.closeds.remove(channelContext);
        } else {
          channelContext = new ClientChannelContext(clientTioConfig, asynchronousSocketChannel);
          channelContext.setServerNode(serverNode);
        }

        channelContext.setBindIp(bindIp);
        channelContext.setBindPort(bindPort);

        channelContext.getReconnCount().set(0);
        channelContext.setClosed(false);
        isConnected = true;

        attachment.setChannelContext(channelContext);

        // clientTioConfig.ips.bind(channelContext);
        clientTioConfig.connecteds.add(channelContext);

        ReadCompletionHandler readCompletionHandler = new ReadCompletionHandler(channelContext);
        VirtualBuffer vBuffer = BufferPoolUtils.allocateRequest(channelContext.getReadBufferSize());
        ByteBuffer readByteBuffer = vBuffer.buffer();
        readByteBuffer.position(0);
        readByteBuffer.limit(readByteBuffer.capacity());
        asynchronousSocketChannel.read(readByteBuffer, vBuffer, readCompletionHandler);

        log.info("connected to {}", serverNode);
        if (isConnected && !isReconnect) {
          channelContext.stat.setTimeFirstConnected(SystemTimer.currTime);
        }
      } else {
        log.error(throwable.toString(), throwable);
        if (channelContext == null) {
          ReconnConf reconnConf = clientTioConfig.getReconnConf();
          if (reconnConf != null) {
            channelContext = new ClientChannelContext(clientTioConfig, asynchronousSocketChannel);
            channelContext.setServerNode(serverNode);
          }
        }

        if (!isReconnect) // 不是重连，则是第一次连接
        {
          if (channelContext != null) {
            attachment.setChannelContext(channelContext);
          }
        }
        boolean f = ReconnConf.put(channelContext);
        if (!f) {
          Tio.close(channelContext, null, "不需要重连，关闭该连接", true, false, ChannelCloseCode.CLIENT_CONNECTION_FAIL);
        }
      }
    } catch (Throwable e) {
      log.error(e.toString(), e);
    } finally {
      if (attachment.getCountDownLatch() != null) {
        attachment.getCountDownLatch().countDown();
      }

      try {
        if (channelContext != null) {
          channelContext.setReconnect(isReconnect);
          if (SslUtils.isSsl(channelContext.tioConfig)) {
            if (isConnected) {
              // channelContext.sslFacadeContext.beginHandshake();
              SslFacadeContext sslFacadeContext = new SslFacadeContext(channelContext);
              sslFacadeContext.beginHandshake();
            } else {
              if (clientAioListener != null) {
                if (isConnected) {
                  channelContext.stat.heartbeatTimeoutCount.set(0);
                  channelContext.setCloseCode(ChannelCloseCode.INIT_STATUS);
                }
                clientAioListener.onAfterConnected(channelContext, isConnected, isReconnect);
              }
            }
          } else {
            if (clientAioListener != null) {
              clientAioListener.onAfterConnected(channelContext, isConnected, isReconnect);
            }
          }

          TioConfig tioConfig = channelContext.tioConfig;
          if (CollUtil.isNotEmpty(tioConfig.ipStats.durationList)) {
            try {
              for (Long v : tioConfig.ipStats.durationList) {
                IpStat ipStat = tioConfig.ipStats.get(v, channelContext);
                ipStat.getRequestCount().incrementAndGet();
                tioConfig.getIpStatListener().onAfterConnected(channelContext, isConnected, isReconnect, ipStat);
              }
            } catch (Exception e) {
              log.error(e.toString(), e);
            }
          }
        }
      } catch (Throwable e1) {
        log.error(e1.toString(), e1);
      }
    }
  }
}
