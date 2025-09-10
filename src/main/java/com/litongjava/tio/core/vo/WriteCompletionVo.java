package com.litongjava.tio.core.vo;

import java.nio.ByteBuffer;

import lombok.Data;

@Data
public class WriteCompletionVo {
  private ByteBuffer byteBuffer;
  private Object obj;

  // 新增：写完后是否自动释放（仅对 direct buffer 有效）
  private boolean autoRelease;

  // 新增：池化归还回调（优先于 autoRelease）
  private Runnable returnToPool;

  public WriteCompletionVo(ByteBuffer byteBuffer, Object obj) {
    this.byteBuffer = byteBuffer;
    this.obj = obj;
  }

  public WriteCompletionVo(ByteBuffer byteBuffer, Object obj, boolean autoRelease) {
    this.byteBuffer = byteBuffer;
    this.obj = obj;
    this.autoRelease = autoRelease;
  }

  public WriteCompletionVo(ByteBuffer byteBuffer, Object obj, Runnable returnToPool) {
    this.byteBuffer = byteBuffer;
    this.obj = obj;
    this.returnToPool = returnToPool;
  }

  public WriteCompletionVo(ByteBuffer byteBuffer, Object obj, boolean autoRelease, Runnable returnToPool) {
    this.byteBuffer = byteBuffer;
    this.obj = obj;
    this.autoRelease = autoRelease;
    this.returnToPool = returnToPool;
  }
}
