package org.FantaDB.LibFanta.KV.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class FileAppendOnlyStorage implements AppendOnlyStorage {
    private final FileChannel channel;
    private final StorageRecordCodec codec =  new StorageRecordCodec();

    public FileAppendOnlyStorage(Path path) throws IOException {
        this.channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        channel.position(channel.size()); // start appending at end
    }

    @Override
    public long append(byte[] key, byte[] value, boolean isTombstone) throws IOException {
        StorageRecord record = new StorageRecord(key, value, isTombstone);
        byte[] serializedRecord = codec.serialize(record);
        long offset = channel.position();

        ByteBuffer buffer = ByteBuffer.wrap(serializedRecord);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }

        return offset;
    }


    @Override
    public StorageRecord read(long offset) throws IOException {
        channel.position(offset);

        ByteBuffer lenBuffer = ByteBuffer.allocate(4);
        channel.read(lenBuffer);
        lenBuffer.flip();
        int recordLength = lenBuffer.getInt();

        ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);
        channel.read(recordBuffer);
        recordBuffer.flip();

        return codec.deserialize(recordBuffer);
    }

    @Override
    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
