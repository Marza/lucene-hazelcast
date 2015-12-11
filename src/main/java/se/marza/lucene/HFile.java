package se.marza.lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.apache.lucene.util.Accountable;

/**
 * @author Marza
 */
public class HFile implements Accountable, IdentifiedDataSerializable {

    protected ArrayList<byte[]> buffers = new ArrayList<>();
    long length;
    volatile HazelcastDirectory directory;
    protected long sizeInBytes;

    // File used as buffer, in no HazelcastDirectory
    public HFile() {
    }

    public HFile(HazelcastDirectory directory) {
        this.directory = directory;
    }

    // For non-stream access from thread that might be concurrent with writing
    public synchronized long getLength() {
        return length;
    }

    protected synchronized void setLength(long length) {
        this.length = length;
    }

    protected final byte[] addBuffer(int size) {
        byte[] buffer = newBuffer(size);
        synchronized (this) {
            buffers.add(buffer);
            sizeInBytes += size;
        }

        if (directory != null) {
            directory.sizeInBytes.getAndAdd(size);
        }
        return buffer;
    }

    protected final synchronized byte[] getBuffer(int index) {
        return buffers.get(index);
    }

    protected final synchronized int numBuffers() {
        return buffers.size();
    }

    /**
     * Expert: allocate a new buffer.
     * Subclasses can allocate differently.
     *
     * @param size size of allocated buffer.
     * @return allocated buffer.
     */
    protected byte[] newBuffer(int size) {
        return new byte[size];
    }

    @Override
    public synchronized long ramBytesUsed() {
        return sizeInBytes;
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(length=" + length + ")";
    }

    @Override
    public int hashCode() {
        int h = (int) (length ^ (length >>> 32));
        for (byte[] block : buffers) {
            h = 31 * h + Arrays.hashCode(block);
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        HFile other = (HFile) obj;
        if (length != other.length) return false;
        if (buffers.size() != other.buffers.size()) {
            return false;
        }
        for (int i = 0; i < buffers.size(); i++) {
            if (!Arrays.equals(buffers.get(i), other.buffers.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(length);
        out.writeLong(sizeInBytes);
        out.writeObject(buffers);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        length = in.readLong();
        sizeInBytes = in.readLong();
        buffers = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return HazelcastDataSerializableFactory.FACTORY_ID;
    }

    @Override
    public int getId() {
        return HazelcastDataSerializableFactory.HFILE_TYPE;
    }

    public static final class HazelcastDataSerializableFactory implements DataSerializableFactory {

        public static final int FACTORY_ID = 1;
        public static final int HFILE_TYPE = 1;

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == HFILE_TYPE) {
                return new HFile();
            } else {
                return null;
            }
        }
    }
}