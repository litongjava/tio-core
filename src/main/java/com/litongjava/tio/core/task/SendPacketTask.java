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
import com.litongjava.enhance.buffer.Buffers;
import com.litongjava.enhance.channel.EnhanceAsynchronousServerChannel;
import com.litongjava.tio.consts.TioCoreConfigKeys;
import com.litongjava.tio.core.ChannelContext;
import com.litongjava.tio.core.ChannelContext.CloseCode;
import com.litongjava.tio.core.Tio;
import com.litongjava.tio.core.TioConfig;
import com.litongjava.tio.core.WriteCompletionHandler;
import com.litongjava.tio.core.intf.AioHandler;
import com.litongjava.tio.core.ssl.SslUtils;
import com.litongjava.tio.core.ssl.SslVo;
import com.litongjava.tio.core.utils.TioUtils;
import com.litongjava.tio.core.vo.WriteCompletionVo;
import com.litongjava.tio.utils.environment.EnvUtils;

import lombok.extern.slf4j.Slf4j;

/**
 * Send data to client
 * 
 * @author Tong Li
 */
@Slf4j
public class SendPacketTask {

  private final static boolean DIAGNOSTIC_LOG_ENABLED = EnvUtils.getBoolean(TioCoreConfigKeys.TIO_CORE_DIAGNOSTIC,
      false);

  public boolean canSend = true;
  private ChannelContext channelContext = null;
  private TioConfig tioConfig = null;
  private AioHandler aioHandler = null;
  private boolean isSsl = false;

//类内新增（开关：是否把 heap buffer 强制改为 direct）
  private static final boolean FORCE_DIRECT = EnvUtils.getBoolean("tio.force_direct_out", true);

  public SendPacketTask(ChannelContext channelContext) {
    this.channelContext = channelContext;
    this.tioConfig = channelContext.tioConfig;
    this.aioHandler = tioConfig.getAioHandler();
    this.isSsl = SslUtils.isSsl(tioConfig);
  }

//类内工具：确保发出的 buffer 为 direct（如已是 direct 直接复用；否则池借并拷贝）
  private ByteBuffer ensureDirect(ByteBuffer src) {
    if (src.isDirect())
      return src;
    if (!FORCE_DIRECT)
      return src; // 不强制时，保持 heap
    // 拷贝到池借 direct
    int remaining = src.remaining();
    ByteBuffer direct = Buffers.DIRECT_POOL.borrow(remaining);
    direct.clear();
    direct.put(src.duplicate()); // 拷贝数据
    direct.flip();
    return direct;
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
      // ☆ 关键：确保用于写出的 buffer 为 direct
      byteBuffer = ensureDirect(byteBuffer);
      return byteBuffer;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean sendPacket(Packet packet) {
    if (DIAGNOSTIC_LOG_ENABLED) {
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
              Tio.close(channelContext, "An exception occurred during SSL encryption.", CloseCode.SSL_ENCRYPTION_ERROR);
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
        ByteBuffer buf = Buffers.DIRECT_POOL.borrow(64 * 1024); // 池借
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
              Tio.close(channelContext, "Failed to encrypt data using ssl", CloseCode.SSL_ENCRYPTION_ERROR);
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
          Buffers.DIRECT_POOL.giveBack(buf); // 归还
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

    // ☆ 为 direct buffer 提供归还回调（写完由 WriteCompletionHandler 调）
    Runnable returnToPool = null;
    if (byteBuffer.isDirect()) {
      final ByteBuffer toReturn = byteBuffer;
      returnToPool = () -> Buffers.DIRECT_POOL.giveBack(toReturn);
    }

    // WriteCompletionVo：支持 returnToPool 参数
    WriteCompletionVo writeCompletionVo = new WriteCompletionVo(byteBuffer, packets, false, returnToPool);
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
              Tio.close(channelContext, "An exception occurred during SSL encryption.", CloseCode.SSL_ENCRYPTION_ERROR);
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