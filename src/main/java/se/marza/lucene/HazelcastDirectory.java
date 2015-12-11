package se.marza.lucene;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Marza
 */
public class HazelcastDirectory extends BaseDirectory implements Accountable {
    private static final Logger LOGGER = LoggerFactory.getLogger(HazelcastDirectory.class);
    private static final String MAP_NAME = "hazelcastDirectory";

    protected static IMap<String, HFile> fileMap;
    protected final AtomicLong sizeInBytes = new AtomicLong();

    //@Value("${hazelcast.members}")
    private String members = "127.0.0.1:8085";

    public HazelcastDirectory() {
        // TODO: develop Hazelcast Lock Factory
        super(new SingleInstanceLockFactory());

        Config config = new Config("lucene-hazelcastDirectory-1.0");

        // near cache config
        /*NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInvalidateOnChange(true);
        config.getMapConfig(MAP_NAME).setNearCacheConfig(nearCacheConfig);*/
        //config.getMapConfig(MAP_NAME).setInMemoryFormat(InMemoryFormat.BINARY);

        // serialization
        config.getSerializationConfig().addDataSerializableFactory(
                HFile.HazelcastDataSerializableFactory.FACTORY_ID,
                new HFile.HazelcastDataSerializableFactory());

        // network config
        config.getNetworkConfig().setPort(8085);
        config.getNetworkConfig().setPortCount(2);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember(members);

        // group config
        config.getGroupConfig().setName("lucene-hazelcast");
        config.getGroupConfig().setPassword("password");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        fileMap = instance.getMap(MAP_NAME);
    }

    @Override
    public String[] listAll() throws IOException {
        ensureOpen();
        return fileMap.keySet().toArray(new String[fileMap.size()]);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        //LOGGER.debug("deleteFile={}", name);
        ensureOpen();
        HFile file = fileMap.remove(name);
        if (file != null) {
            file.directory = null;
            sizeInBytes.addAndGet(-file.sizeInBytes);
        } else {
            throw new FileNotFoundException(name);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        //LOGGER.debug("fileLength={}", name);
        ensureOpen();
        HFile file = fileMap.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
        return file.getLength();
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        //LOGGER.debug("createOutput={}", name);
        ensureOpen();
        HFile file = new HFile(this);
        HFile existing = fileMap.remove(name);
        if (existing != null) {
            sizeInBytes.addAndGet(-existing.sizeInBytes);
            existing.directory = null;
        }
        fileMap.set(name, file);
        return new HOutputStream(name, file);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        //LOGGER.debug("sync=[{}]", names);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        //LOGGER.debug("createInput={}", name);
        ensureOpen();
        HFile file = fileMap.get(name);
        if (file == null) {
            throw new FileNotFoundException(name);
        }
        return new HInputStream(file, name);
    }

    @Override
    public void close() throws IOException {
        //LOGGER.debug("close");
        isOpen = false;
        fileMap.clear();
    }

    @Override
    public void renameFile(String source, String dest) throws IOException {
        //LOGGER.debug("renameFile={}->{}", source, dest);
        ensureOpen();
        HFile file = fileMap.get(source);
        if (file == null) {
            throw new FileNotFoundException(source);
        }
        fileMap.set(dest, file);
        fileMap.remove(source);
    }

    @Override
    public long ramBytesUsed() {
        ensureOpen();
        return sizeInBytes.get();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return Accountables.namedAccountables("file", fileMap);
    }
}