package com.litongjava.tio.core.ssl.facade;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.litongjava.tio.core.ChannelContext;
import com.litongjava.tio.core.Node;
import com.litongjava.tio.core.ssl.SslVo;
import com.litongjava.tio.core.utils.ByteBufferUtils;

/**
 * @author tanyaowu
 */
public class SSLFacade implements ISSLFacade {
  private static final Logger log = LoggerFactory.getLogger(SSLFacade.class);
  private AtomicLong sslSeq = new AtomicLong();

  private Handshaker _handshaker;
  private IHandshakeCompletedListener _hcl;
  private final Worker _worker;
  private boolean _clientMode;
  private ChannelContext channelContext;

  public SSLFacade(ChannelContext channelContext, SSLContext context, boolean client, boolean clientAuthRequired,
      ITaskHandler taskHandler) {
    this.channelContext = channelContext;

    final String who = client ? "client" : "server";

    // 关键修复：带 peerHost/peerPort 创建 SSLEngine（启用 SNI）
    SSLEngine engine = makeSSLEngine(context, client, clientAuthRequired, channelContext);

    Buffers buffers = new Buffers(engine.getSession(), channelContext);
    _worker = new Worker(who, engine, buffers, channelContext);
    _handshaker = new Handshaker(client, _worker, taskHandler, channelContext);
    _clientMode = client;
  }

  @Override
  public boolean isClientMode() {
    return _clientMode;
  }

  @Override
  public void setHandshakeCompletedListener(IHandshakeCompletedListener hcl) {
    _hcl = hcl;
    attachCompletionListener();
  }

  @Override
  public void setSSLListener(ISSLListener l) {
    _worker.setSSLListener(l);
  }

  @Override
  public void setCloseListener(ISessionClosedListener l) {
    _worker.setSessionClosedListener(l);
  }

  @Override
  public void beginHandshake() throws SSLException {
    _handshaker.begin();
  }

  @Override
  public boolean isHandshakeCompleted() {
    return (_handshaker == null) || _handshaker.isFinished();
  }

  @Override
  public void encrypt(SslVo sslVo) throws SSLException {
    long seq = sslSeq.incrementAndGet();

    ByteBuffer src = sslVo.getByteBuffer();
    ByteBuffer[] byteBuffers = ByteBufferUtils.split(src, 1024 * 8);
    if (byteBuffers == null) {
      log.debug("{}, 准备, SSL加密{}, 明文:{}", channelContext, channelContext.getId() + "_" + seq, sslVo);
      SSLEngineResult result = _worker.wrap(sslVo, sslVo.getByteBuffer());
      log.debug("{}, 完成, SSL加密{}, 明文:{}, 结果:{}", channelContext, channelContext.getId() + "_" + seq, sslVo, result);

    } else {
      log.debug("{}, 准备, SSL加密{}, 包过大，被拆成了[{}]个包进行发送, 明文:{}", channelContext, channelContext.getId() + "_" + seq,
          byteBuffers.length, sslVo);
      ByteBuffer[] encryptedByteBuffers = new ByteBuffer[byteBuffers.length];
      int alllen = 0;
      for (int i = 0; i < byteBuffers.length; i++) {
        SslVo sslVo1 = new SslVo(byteBuffers[i], sslVo.getObj());
        SSLEngineResult result = _worker.wrap(sslVo1, byteBuffers[i]);
        ByteBuffer encryptedByteBuffer = sslVo1.getByteBuffer();
        encryptedByteBuffers[i] = encryptedByteBuffer;
        alllen += encryptedByteBuffer.limit();
        log.debug("{}, 完成, SSL加密{}, 明文:{}, 拆包[{}]的结果:{}", channelContext, channelContext.getId() + "_" + seq, sslVo,
            (i + 1), result);
      }

      ByteBuffer encryptedByteBuffer = ByteBuffer.allocate(alllen);
      for (int i = 0; i < encryptedByteBuffers.length; i++) {
        encryptedByteBuffer.put(encryptedByteBuffers[i]);
      }
      encryptedByteBuffer.flip();
      sslVo.setByteBuffer(encryptedByteBuffer);
    }
  }

  @Override
  public void decrypt(ByteBuffer byteBuffer) throws SSLException {
    long seq = sslSeq.incrementAndGet();
    log.debug("{}, 准备, SSL解密{}, 密文:{}", channelContext, channelContext.getId() + "_" + seq, byteBuffer);
    SSLEngineResult result = _worker.unwrap(byteBuffer);
    log.debug("{}, 完成, SSL解密{}, 密文:{}, 结果:{}", channelContext, channelContext.getId() + "_" + seq, byteBuffer, result);
    _handshaker.handleUnwrapResult(result);
  }

  @Override
  public void close() {
    _worker.close(true);
  }

  @Override
  public boolean isCloseCompleted() {
    return _worker.isCloseCompleted();
  }

  @Override
  public void terminate() {
    _worker.close(false);
  }

  private void attachCompletionListener() {
    _handshaker.addCompletedListener(new IHandshakeCompletedListener() {
      @Override
      public void onComplete() {
        if (_hcl != null) {
          _hcl.onComplete();
          _hcl = null;
        }
      }
    });
  }

  /**
   * 关键修复点： 1) client 模式下使用 createSSLEngine(peerHost, peerPort) 以启用 SNI 2) 可选：启用
   * HTTPS endpoint identification（严格域名校验）
   */
  private SSLEngine makeSSLEngine(SSLContext context, boolean client, boolean clientAuthRequired, ChannelContext cc) {
    SSLEngine engine;

    if (client) {
      String peerHost = null;
      int peerPort = -1;

      try {
        Node serverNode = null;
        // 客户端一般是 serverNode
        if (cc != null) {
          serverNode = cc.getServerNode();
        }
        if (serverNode != null) {
          peerHost = serverNode.getIp();
          peerPort = serverNode.getPort();
          log.info("{}, SSLEngine peerHost={}, peerPort={}", channelContext, peerHost, peerPort);

        }
      } catch (Throwable ignore) {
      }

      if (peerHost != null && peerPort > 0) {
        engine = context.createSSLEngine(peerHost, peerPort);
      } else {
        // 兜底：没有 host/port 也能跑，但可能缺 SNI（不推荐）
        engine = context.createSSLEngine();
        log.warn("{}, createSSLEngine() without peerHost/peerPort, SNI may be missing", channelContext);
      }

      engine.setUseClientMode(true);

      // 开启 HTTPS 域名校验（建议开启；如果你 Node.ip 真的是 IP 而不是域名，这里会校验失败）
      try {
        SSLParameters params = engine.getSSLParameters();
        params.setEndpointIdentificationAlgorithm("HTTPS");
        engine.setSSLParameters(params);
      } catch (Throwable t) {
        // 某些老环境可能不支持，忽略
        log.debug("{}, unable to set endpointIdentificationAlgorithm: {}", channelContext, t.toString());
      }

    } else {
      engine = context.createSSLEngine();
      engine.setUseClientMode(false);
      engine.setNeedClientAuth(clientAuthRequired);
    }

    return engine;
  }
}
