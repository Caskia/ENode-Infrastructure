﻿using ECommon.Storage;

namespace EQueue.Broker
{
    public static class Extensions
    {
        public static BufferLogRecord TryReadRecordBufferAt(this ChunkReader chunkReader, long position)
        {
            return chunkReader.TryReadAt(position, recordBuffer =>
            {
                var record = new BufferLogRecord();
                record.ReadFrom(recordBuffer);
                return record;
            });
        }
    }
}
