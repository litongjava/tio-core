package com.litongjava.tio.core.ssl;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.litongjava.tio.core.ChannelContext;
import com.litongjava.tio.core.ssl.facade.DefaultTaskHandler;
import com.litongjava.tio.core.ssl.facade.ISSLFacade;
import com.litongjava.tio.core.ssl.facade.SSLFacade;

/**
 * @author tanyaowu
 *
 */
public class SslFacadeContext {
  private static Logger log = LoggerFactory.getLogger(SslFacadeContext.class);

  private ChannelContext channelContext = null;

  private SSLContext sslContext;

  private ISSLFacade sslFacade = null;

  //ssl握手是否已经完成, true: 已经完成， false: 还没有完成
  private boolean isHandshakeCompleted = false;

  /**
   * 
   * @param channelContext
   * @throws Exception
   */
  public SslFacadeContext(ChannelContext channelContext) throws Exception {
    this.channelContext = channelContext;
    this.channelContext.setSslFacadeContext(this);

    this.isHandshakeCompleted = false;

    sslContext = SSLContext.getInstance("TLS");
    KeyManager[] keyManagers = channelContext.tioConfig.sslConfig.getKeyManagerFactory().getKeyManagers();
    TrustManager[] trustManagers = channelContext.tioConfig.sslConfig.getTrustManagerFactory().getTrustManagers();
    sslContext.init(keyManagers, trustManagers, null);

    DefaultTaskHandler taskHandler = new DefaultTaskHandler();

    boolean isClient = true;
    if (this.channelContext.isServer()) { //server mode
      isClient = false;
    }

    sslFacade = new SSLFacade(this.channelContext, sslContext, isClient, false, taskHandler);
    sslFacade.setHandshakeCompletedListener(new SslHandshakeCompletedListener(this.channelContext));
    sslFacade.setSSLListener(new SslListener(this.channelContext));
    sslFacade.setCloseListener(new SslSessionClosedListener(this.channelContext));
  }

  /**
   * 
   * @throws Exception
   */
  public void beginHandshake() throws Exception {
    log.info("Start SSL Handshake {}", channelContext);
    sslFacade.beginHandshake();
  }

  public boolean isHandshakeCompleted() {
    return isHandshakeCompleted;
  }

  public void setHandshakeCompleted(boolean isHandshakeCompleted) {
    this.isHandshakeCompleted = isHandshakeCompleted;
  }

  public ChannelContext getChannelContext() {
    return channelContext;
  }

  public SSLContext getSslContext() {
    return sslContext;
  }

  public ISSLFacade getSslFacade() {
    return sslFacade;
  }

}
