package com.litongjava.tio.core.vo;

import java.nio.ByteBuffer;

import lombok.Data;

@Data
public class WriteCompletionVo {
  private ByteBuffer byteBuffer;
  private Object obj;
  private Integer totalWritten;

  public WriteCompletionVo(ByteBuffer byteBuffer, Object obj) {
    this.byteBuffer = byteBuffer;
    this.obj = obj;
  }
}
