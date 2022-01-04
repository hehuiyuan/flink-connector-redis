package org.apache.flink.streaming.connectors.redis.common.source;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.caffinitas.ohc.CacheSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CacheRowSerializer implements CacheSerializer<CacheRow>, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CacheRowSerializer.class);

    private static final ThreadLocal<Kryo> kryos =
            new ThreadLocal<Kryo>() {
                protected Kryo initialValue() {
                    Kryo kryo = new Kryo();
                    kryo.setRegistrationRequired(false);
                    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
                    // Configure the Kryo instance.
                    return kryo;
                }
            };

    @Override
    public void serialize(CacheRow row, ByteBuffer buf) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream)) {
            kryos.get().writeObject(output, row);
            output.flush();
            byte[] bytes = byteArrayOutputStream.toByteArray();
            buf.putInt(bytes.length);
            buf.put(bytes);
        } catch (IOException e) {
            LOG.error("Serialize row error. Row is " + row.toString() + ".");
        }
    }

    @Override
    public CacheRow deserialize(ByteBuffer buf) {
        int size = buf.getInt();
        byte[] bytes = new byte[size];
        buf.get(bytes);
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                Input input = new Input(byteArrayInputStream)) {
            return kryos.get().readObject(input, CacheRow.class);
        } catch (IOException e) {
            LOG.error(
                    "Deserialize error. ByteBuffer is "
                            + new String(buf.array(), StandardCharsets.UTF_8)
                            + ".");
        }
        return null;
    }

    @Override
    public int serializedSize(CacheRow row) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream); ) {
            kryos.get().writeObject(output, row);
            output.flush();
            byte[] bytes = byteArrayOutputStream.toByteArray();
            return bytes.length + 4;
        } catch (IOException e) {
            LOG.error("Serialize row error. Row is " + row.toString() + ".");
        }
        return 0;
    }
}
