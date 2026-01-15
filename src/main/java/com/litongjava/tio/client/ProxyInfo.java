package com.litongjava.tio.client;

import com.litongjava.tio.core.Node;

public class ProxyInfo {
  private Node proxyNode;
  private ProxyType proxyType;
  private String proxyUser;
  private String proxyPass;

  public ProxyInfo(Node targetNode, ProxyType proxyType, String proxyUser, String proxyPass) {
    super();
    this.proxyNode = targetNode;
    this.proxyType = proxyType;
    this.proxyUser = proxyUser;
    this.proxyPass = proxyPass;
  }

  public Node getProxyNode() {
    return proxyNode;
  }

  public void setProxyNode(Node proxyNode) {
    this.proxyNode = proxyNode;
  }

  public ProxyType getProxyType() {
    return proxyType;
  }

  public void setProxyType(ProxyType proxyType) {
    this.proxyType = proxyType;
  }

  public String getProxyUser() {
    return proxyUser;
  }

  public void setProxyUser(String proxyUser) {
    this.proxyUser = proxyUser;
  }

  public String getProxyPass() {
    return proxyPass;
  }

  public void setProxyPass(String proxyPass) {
    this.proxyPass = proxyPass;
  }
}
