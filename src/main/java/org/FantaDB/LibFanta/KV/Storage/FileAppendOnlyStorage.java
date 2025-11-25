package org.FantaDB.LibFanta.KV.Storage;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class FileAppendOnlyStorage implements AppendOnlyStorage, Iterable<StorageRecordOffset> {
    private final FileChannel channel;
    private final StorageRecordCodec codec =  new StorageRecordCodec();

    private record ReadResult(StorageRecord record, int sizeBytes) {
    }

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
        return readFromOffset(offset).record;
    }

    @Override
    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public @NotNull Iterator<StorageRecordOffset> iterator() {
        return new Iterator<> () {
            private long position = 0;
            private long fileSize;

            {
                try {
                    fileSize = channel.size();
                } catch (IOException e) {
                    throw new RuntimeException("Failed to get file size from channel", e);
                }
            }

            @Override
            public boolean hasNext() {
                return position < fileSize;
            }

            @Override
            public StorageRecordOffset next() {
                try {
                    ReadResult readResult = readFromOffset(position);
                    StorageRecordOffset result = new StorageRecordOffset(position, readResult.record());
                    position += 4 + readResult.sizeBytes();
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException("Failed to read record at offset " + position, e);
                }
            }
        };
    }

    private ReadResult readFromOffset(long offset) throws IOException {
        channel.position(offset);

        ByteBuffer lenBuffer = ByteBuffer.allocate(4);
        channel.read(lenBuffer);
        lenBuffer.flip();
        int recordLength = lenBuffer.getInt();

        ByteBuffer recordBuffer = ByteBuffer.allocate(recordLength);
        channel.read(recordBuffer);
        recordBuffer.flip();

        StorageRecord record = codec.deserialize(recordBuffer);
        return new ReadResult(record, recordLength);
    }
}
