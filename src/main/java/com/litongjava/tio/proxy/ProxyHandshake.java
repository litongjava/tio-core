package com.litongjava.tio.proxy;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class ProxyHandshake {

  public static void httpConnect(AsynchronousSocketChannel ch, String targetHost, int targetPort, String user,
      String pass) throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("CONNECT ").append(targetHost).append(":").append(targetPort).append(" HTTP/1.1\r\n");
    sb.append("Host: ").append(targetHost).append(":").append(targetPort).append("\r\n");
    sb.append("Proxy-Connection: Keep-Alive\r\n");

    if (user != null && pass != null) {
      String token = Base64.getEncoder().encodeToString((user + ":" + pass).getBytes(StandardCharsets.UTF_8));
      sb.append("Proxy-Authorization: Basic ").append(token).append("\r\n");
    }
    sb.append("\r\n");

    writeFully(ch, ByteBuffer.wrap(sb.toString().getBytes(StandardCharsets.UTF_8)));

    ByteBuffer in = ByteBuffer.allocate(8192);
    int n = ch.read(in).get();
    if (n <= 0)
      throw new RuntimeException("proxy CONNECT no response");
    in.flip();
    String resp = new String(in.array(), 0, in.remaining(), StandardCharsets.UTF_8);
    if (!resp.contains(" 200 ")) {
      throw new RuntimeException("proxy CONNECT failed: " + resp);
    }
  }

  public static void socks5Connect(AsynchronousSocketChannel ch, String targetHost, int targetPort, String user,
      String pass) throws Exception {
    // 先只实现无鉴权；需要鉴权我再给你补 username/password 子协商
    writeFully(ch, ByteBuffer.wrap(new byte[] { 0x05, 0x01, 0x00 }));

    ByteBuffer resp = ByteBuffer.allocate(2);
    readFully(ch, resp);
    resp.flip();
    byte ver = resp.get();
    byte method = resp.get();
    if (ver != 0x05 || method != 0x00) {
      throw new RuntimeException("socks5 method not accepted: ver=" + ver + ", method=" + method);
    }

    byte[] host = targetHost.getBytes(StandardCharsets.UTF_8);
    ByteBuffer req = ByteBuffer.allocate(4 + 1 + host.length + 2);
    req.put((byte) 0x05);
    req.put((byte) 0x01);
    req.put((byte) 0x00);
    req.put((byte) 0x03);
    req.put((byte) host.length);
    req.put(host);
    req.putShort((short) targetPort);
    req.flip();
    writeFully(ch, req);

    ByteBuffer hdr = ByteBuffer.allocate(4);
    readFully(ch, hdr);
    hdr.flip();
    byte rver = hdr.get();
    byte rep = hdr.get();
    hdr.get();
    byte atyp = hdr.get();
    if (rver != 0x05 || rep != 0x00) {
      throw new RuntimeException("socks5 connect failed, rep=" + rep);
    }

    int addrLen;
    if (atyp == 0x01)
      addrLen = 4;
    else if (atyp == 0x04)
      addrLen = 16;
    else if (atyp == 0x03) {
      ByteBuffer l = ByteBuffer.allocate(1);
      readFully(ch, l);
      l.flip();
      addrLen = l.get() & 0xff;
    } else
      throw new RuntimeException("unknown atyp=" + atyp);

    ByteBuffer rest = ByteBuffer.allocate(addrLen + 2);
    readFully(ch, rest);
  }

  private static void writeFully(AsynchronousSocketChannel ch, ByteBuffer buf) throws Exception {
    while (buf.hasRemaining()) {
      ch.write(buf).get();
    }
  }

  private static void readFully(AsynchronousSocketChannel ch, ByteBuffer buf) throws Exception {
    while (buf.hasRemaining()) {
      int n = ch.read(buf).get();
      if (n < 0)
        throw new RuntimeException("channel closed");
    }
  }
}
