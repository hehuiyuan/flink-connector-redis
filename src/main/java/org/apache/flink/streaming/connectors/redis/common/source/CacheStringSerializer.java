package org.apache.flink.streaming.connectors.redis.common.source;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.caffinitas.ohc.CacheSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CacheStringSerializer implements CacheSerializer<String> {
    private static final ThreadLocal<Kryo> kryos =
            new ThreadLocal<Kryo>() {
                protected Kryo initialValue() {
                    Kryo kryo = new Kryo();
                    // Configure the Kryo instance.
                    return kryo;
                }
            };

    @Override
    public void serialize(String s, ByteBuffer buf) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream); ) {
            kryos.get().writeClassAndObject(output, s);
            output.flush();
            byte[] bytes = byteArrayOutputStream.toByteArray();
            buf.putInt(bytes.length);
            buf.put(bytes);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String deserialize(ByteBuffer buf) {
        int size = buf.getInt();
        byte[] bytes = new byte[size];
        buf.get(bytes);
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
                Input input = new Input(byteArrayInputStream)) {
            return (String) kryos.get().readClassAndObject(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public int serializedSize(String s) {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                Output output = new Output(byteArrayOutputStream)) {
            kryos.get().writeClassAndObject(output, s);
            output.flush();
            byte[] bytes = byteArrayOutputStream.toByteArray();
            return bytes.length + 4;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }
}
