package se.marza.lucene;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;

/**
 * @author Marza
 */
public class HOutputStream extends IndexOutput implements Accountable {
    static final int BUFFER_SIZE = 1024;

    private HFile file;
    private String name;
    private byte[] currentBuffer;
    private int currentBufferIndex;

    private int bufferPosition;
    private long bufferStart;
    private int bufferLength;

    private final Checksum crc;

    public HOutputStream(String name, HFile file) {
        super("HOutputStream(name=\"" + name + "\")");
        this.name = name;
        this.file = file;
        currentBufferIndex = -1;
        currentBuffer = null;
        boolean checksum = true; // hardcoded for now
        if (checksum) {
            crc = new BufferedChecksum(new CRC32());
        } else {
            crc = null;
        }
    }

    /**
     * Copy the current contents of this buffer to the named output.
     */
    /* TODO: WAS HERE
    public void writeTo(IndexOutput out) throws IOException {
        flush();
        final long end = file.length;
        long pos = 0;
        int buffer = 0;
        while (pos < end) {
            int length = BUFFER_SIZE;
            long nextPos = pos + length;
            if (nextPos > end) {                        // at the last buffer
                length = (int) (end - pos);
            }
            out.writeBytes(file.getBuffer(buffer++), length);
            pos = nextPos;
        }
        HazelcastDirectory.fileMap.put(name, file);
    }*/

    /**
     * Copy the current contents of this buffer to output
     * byte array
     */
    /* TODO: WAS HERE
    public void writeTo(byte[] bytes, int offset) throws IOException {
        flush();
        final long end = file.length;
        long pos = 0;
        int buffer = 0;
        int bytesUpto = offset;
        while (pos < end) {
            int length = BUFFER_SIZE;
            long nextPos = pos + length;
            if (nextPos > end) {                        // at the last buffer
                length = (int) (end - pos);
            }
            System.arraycopy(file.getBuffer(buffer++), 0, bytes, bytesUpto, length);
            bytesUpto += length;
            pos = nextPos;
        }
        HazelcastDirectory.fileMap.put(name, file);
    }*/

    /**
     * Resets this to an empty file.
     */
    public void reset() {
        currentBuffer = null;
        currentBufferIndex = -1;
        bufferPosition = 0;
        bufferStart = 0;
        bufferLength = 0;
        file.setLength(0);
        if (crc != null) {
            crc.reset();
        }
        //HazelcastDirectory.fileMap.put(name, file); // TODO: was here
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    /* TODO: WAS HERE
    @Override
    public void seek(long pos) throws IOException {
        // set the file length in case we seek back
        // and flush() has not been called yet
        setFileLength();
        if (pos < bufferStart || pos >= bufferStart + bufferLength) {
            currentBufferIndex = (int) (pos / BUFFER_SIZE);
            switchCurrentBuffer();
        }

        bufferPosition = (int) (pos % BUFFER_SIZE);
        HazelcastDirectory.fileMap.put(name, file);
    }*/

    /* TODO: WAS HERE
    @Override
    public long length() {
        return file.length;
    }*/

    @Override
    public void writeByte(byte b) throws IOException {
        if (bufferPosition == bufferLength) {
            currentBufferIndex++;
            switchCurrentBuffer();
        }
        if (crc != null) {
            crc.update(b);
        }
        currentBuffer[bufferPosition++] = b;
        //       OffHeapHazelcastDirectory.fileMap.put(name, file);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int len) throws IOException {
        assert b != null;
        if (crc != null) {
            crc.update(b, offset, len);
        }
        while (len > 0) {
            if (bufferPosition == bufferLength) {
                currentBufferIndex++;
                switchCurrentBuffer();
            }

            int remainInBuffer = currentBuffer.length - bufferPosition;
            int bytesToCopy = len < remainInBuffer ? len : remainInBuffer;
            System.arraycopy(b, offset, currentBuffer, bufferPosition, bytesToCopy);
            offset += bytesToCopy;
            len -= bytesToCopy;
            bufferPosition += bytesToCopy;
        }
        //       OffHeapHazelcastDirectory.fileMap.put(name,file);
    }

    private void switchCurrentBuffer() {
        if (currentBufferIndex == file.numBuffers()) {
            currentBuffer = file.addBuffer(BUFFER_SIZE);
        } else {
            currentBuffer = file.getBuffer(currentBufferIndex);
        }
        bufferPosition = 0;
        bufferStart = (long) BUFFER_SIZE * (long) currentBufferIndex;
        bufferLength = currentBuffer.length;
    }

    private void setFileLength() {
        long pointer = bufferStart + bufferPosition;
        if (pointer > file.length) {
            file.setLength(pointer);
        }
        HazelcastDirectory.fileMap.set(name, file);
    }

    // TODO: had @Override
    public void flush() throws IOException {
        setFileLength();
    }

    @Override
    public long getFilePointer() {
        return currentBufferIndex < 0 ? 0 : bufferStart + bufferPosition;
    }

    /**
     * Returns byte usage of all buffers.
     */
    @Override
    public long ramBytesUsed() {
        return (long) file.numBuffers() * (long) BUFFER_SIZE;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.singleton(Accountables.namedAccountable("file", file));
    }

    @Override
    public long getChecksum() throws IOException {
        if (crc == null) {
            throw new IllegalStateException("internal HOutputStream created with checksum disabled");
        } else {
            return crc.getValue();
        }
    }
}
