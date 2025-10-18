package com.litongjava.tio.core.task;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.LockSupport;

import javax.net.ssl.SSLException;

import com.litongjava.aio.Packet;
import com.litongjava.enhance.channel.EnhanceAsynchronousServerChannel;
import com.litongjava.tio.core.ChannelContext;
import com.litongjava.tio.core.ChannelCloseCode;
import com.litongjava.tio.core.Tio;
import com.litongjava.tio.core.TioConfig;
import com.litongjava.tio.core.WriteCompletionHandler;
import com.litongjava.tio.core.intf.AioHandler;
import com.litongjava.tio.core.pool.BufferPoolUtils;
import com.litongjava.tio.core.ssl.SslUtils;
import com.litongjava.tio.core.ssl.SslVo;
import com.litongjava.tio.core.utils.TioUtils;
import com.litongjava.tio.core.vo.WriteCompletionVo;

import lombok.extern.slf4j.Slf4j;

/**
 * Send data to client
 * 
 * @author Tong Li
 */
@Slf4j
public class SendPacketTask {

  private final static boolean disgnostic = TioConfig.disgnostic;

  public boolean canSend = true;
  private ChannelContext channelContext = null;
  private TioConfig tioConfig = null;
  private AioHandler aioHandler = null;
  private boolean isSsl = false;

  public SendPacketTask(ChannelContext channelContext) {
    this.channelContext = channelContext;
    this.tioConfig = channelContext.tioConfig;
    this.aioHandler = tioConfig.getAioHandler();
    this.isSsl = SslUtils.isSsl(tioConfig);
  }

  private ByteBuffer getByteBuffer(Packet packet) {
    ByteBuffer byteBuffer = packet.getPreEncodedByteBuffer();
    try {
      if (byteBuffer == null) {
        byteBuffer = aioHandler.encode(packet, tioConfig, channelContext);
      }
      if (!byteBuffer.hasRemaining()) {
        byteBuffer.flip();
      }
      return byteBuffer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean sendPacket(Packet packet) {
    if (disgnostic) {
      log.info("send:{},{}", channelContext.getClientNode(), packet);
    }
    // 将数据包加入队列
    channelContext.sendQueue.offer(packet);
    // 如果当前没有发送且队列不为空，则开始发送
    if (channelContext.isSending.compareAndSet(false, true)) {
      Packet nextPacket = channelContext.sendQueue.poll();
      if (nextPacket != null) {
        ByteBuffer byteBuffer = getByteBuffer(nextPacket);
        if (isSsl) {
          if (!packet.isSslEncrypted()) {
            SslVo sslVo = new SslVo(byteBuffer, nextPacket);
            try {
              channelContext.sslFacadeContext.getSslFacade().encrypt(sslVo);
              byteBuffer = sslVo.getByteBuffer();
            } catch (SSLException e) {
              log.error(channelContext.toString() + ", An exception occurred while performing SSL encryption", e);
              Tio.close(channelContext, "An exception occurred during SSL encryption.", ChannelCloseCode.SSL_ENCRYPTION_ERROR);
              return false;
            }
          }
        }

        AsynchronousSocketChannel asc = channelContext.asynchronousSocketChannel;
        File fileBody = packet.getFileBody();
        if (fileBody != null && asc instanceof EnhanceAsynchronousServerChannel) {
          boolean keepConnection = nextPacket.isKeepConnection();
          // send header
          nextPacket.setKeepConnection(true);
          sendByteBuffer(byteBuffer, nextPacket);

          transfer(fileBody, nextPacket, asc);

          if (!keepConnection) {
            Tio.close(channelContext, "Send file finish");
          }
        } else {
          sendByteBuffer(byteBuffer, nextPacket);
        }
      } else {
        channelContext.isSending.set(false);
      }
    }

    return true;
  }

  private void transfer(File fileBody, Packet nextPacket, AsynchronousSocketChannel asc) {
    SocketChannel sc = ((EnhanceAsynchronousServerChannel) asc).getSocketChannel();
    if (!isSsl) {
      // —— 零拷贝 ——（不需要 buffer 池）
      try (FileChannel fc = FileChannel.open(fileBody.toPath(), StandardOpenOption.READ)) {
        long pos = 0, size = fc.size();
        while (pos < size) {
          long sent = fc.transferTo(pos, size - pos, sc);
          if (sent > 0) {
            pos += sent;
          } else {
            LockSupport.parkNanos(1_000);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      // —— SSL：分块读 + 加密 + 写出 ——（改：分配→池借；用完归还）
      try (FileChannel fc = FileChannel.open(fileBody.toPath(), StandardOpenOption.READ)) {
        ByteBuffer buf = BufferPoolUtils.allocate(TioConfig.WRITE_CHUNK_SIZE, 64 * 1024);
        try {
          int readBytes;
          while ((readBytes = fc.read(buf)) != -1) {
            if (readBytes == 0)
              continue;
            buf.flip();

            SslVo sslVo = new SslVo(buf, nextPacket);
            try {
              channelContext.sslFacadeContext.getSslFacade().encrypt(sslVo);
            } catch (SSLException e) {
              log.error("Failed to encrypt data using ssl", e);
              Tio.close(channelContext, "Failed to encrypt data using ssl", ChannelCloseCode.SSL_ENCRYPTION_ERROR);
              break;
            }
            ByteBuffer encrypted = sslVo.getByteBuffer();
            // 同步写出
            while (encrypted.hasRemaining()) {
              sc.write(encrypted);
            }
            buf.clear();
          }
        } finally {
          BufferPoolUtils.clean(buf);
        }
      } catch (IOException e1) {
        e1.printStackTrace();
      }
    }
  }

  /**
   *
   * @param byteBuffer
   * @param packets    Packet or List<Packet>
   * @author tanyaowu
   */
  private void sendByteBuffer(ByteBuffer byteBuffer, Object packets) {
    if (byteBuffer == null) {
      log.error("{},byteBuffer is null", channelContext);
      return;
    }
    if (!TioUtils.checkBeforeIO(channelContext)) {
      return;
    }

    // WriteCompletionVo：支持 returnToPool 参数
    WriteCompletionVo writeCompletionVo = new WriteCompletionVo(byteBuffer, packets);
    WriteCompletionHandler writeCompletionHandler = new WriteCompletionHandler(this.channelContext);
    this.channelContext.asynchronousSocketChannel.write(byteBuffer, writeCompletionVo, writeCompletionHandler);
  }

  public void processSendQueue() {
    // 如果当前没有发送且队列不为空，则开始发送
    if (channelContext.isSending.compareAndSet(false, true)) {
      Packet nextPacket = channelContext.sendQueue.poll();
      if (nextPacket != null) {
        ByteBuffer byteBuffer = getByteBuffer(nextPacket);
        if (isSsl) {
          if (!nextPacket.isSslEncrypted()) {
            SslVo sslVo = new SslVo(byteBuffer, nextPacket);
            try {
              channelContext.sslFacadeContext.getSslFacade().encrypt(sslVo);
              byteBuffer = sslVo.getByteBuffer();
            } catch (SSLException e) {
              log.error(channelContext.toString() + ", An exception occurred while performing SSL encryption", e);
              Tio.close(channelContext, "An exception occurred during SSL encryption.", ChannelCloseCode.SSL_ENCRYPTION_ERROR);
            }
          }
        }
        sendByteBuffer(byteBuffer, nextPacket);
      } else {
        channelContext.isSending.set(false);
      }
    }
  }
}