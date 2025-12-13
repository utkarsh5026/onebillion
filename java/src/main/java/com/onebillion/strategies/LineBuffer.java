package com.onebillion.strategies;

class LineBuffer {
  private static final int DEFAULT_SIZE = 128;
  private final byte[] buffer;
  private final LineReader reader;
  private int position = 0;
  private int filled = 0;

  public LineBuffer(int size, LineReader reader) {
    this.buffer = new byte[size];
    this.reader = reader;
  }

  public LineBuffer(LineReader reader) {
    this(DEFAULT_SIZE, reader);
  }

  public void process(byte b) {
    if (b == '\n' || b == '\r') {
      if (reader != null && position > 0) {
        reader.readLine(buffer, position);
        filled++;
      }
      position = 0;
      return;
    }

    if (position < buffer.length) {
      buffer[position++] = b;
      return;
    }
    throw new IndexOutOfBoundsException("Buffer overflow");
  }

  public int getFilled() {
    return filled;
  }
}
