package se.marza.lucene;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import com.google.common.io.ByteStreams;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;

/**
 * @author Marza
 */
public class HFileMapStore implements MapStore<String, HFile>, MapLoader<String, HFile> {

    private final String path;

    public HFileMapStore(String path) {
        this.path = path;
    }

    @Override
    public void store(String key, HFile value) {
        try {
            File file = new File(path + "/" + key);
            if (Files.exists(file.toPath())) {
                Files.delete(file.toPath());
            }
            Files.createFile(file.toPath());
            for (byte[] bytes : value.buffers) {
                Files.write(file.toPath(), bytes, StandardOpenOption.APPEND);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void storeAll(Map<String, HFile> map) {
        for (Map.Entry<String, HFile> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public void delete(String key) {
        try {
            File file = new File(path + "/" + key);
            if (Files.exists(file.toPath())) {
                Files.delete(file.toPath());
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void deleteAll(Collection<String> keys) {
        keys.forEach(this::delete);
    }

    @Override
    public HFile load(String key) {
        try {
            File file = new File(path + "/" + key);
            if (Files.exists(file.toPath())) {
                HFile hFile = new HFile();
                HOutputStream hOutputStream = new HOutputStream(key, hFile);
                InputStream inputStream = Files.newInputStream(file.toPath());
                ByteStreams.copy(inputStream, new HOutStream(hOutputStream));
                return hFile;
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return null;
    }

    @Override
    public Map<String, HFile> loadAll(Collection<String> keys) {
        Map<String, HFile> map = new HashMap<>(keys.size());
        for (String key : keys) {
            HFile hFile = load(key);
            if (hFile != null) {
                map.put(key, hFile);
            }
        }

        if (!map.isEmpty()) {
            return map;
        }
        return null;
    }

    @Override
    public Iterable<String> loadAllKeys() {
        try {
            Stream<Path> stream = Files.list(new File(path).toPath());
            return () -> stream.map(Path::getFileName)
                    .filter(p -> p != null)
                    .map(p -> p.toFile().getName())
                    .iterator();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static final class HOutStream extends OutputStream {
        private HOutputStream stream;

        public HOutStream(HOutputStream stream) {
            this.stream = stream;
        }

        @Override
        public void write ( int b)throws IOException {
            stream.writeByte((byte) (b & 0xFF));
        }
    }
}
