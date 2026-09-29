package com.aliyun.odps.kafka.connect.sink;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import com.aliyun.odps.kafka.connect.ConnectorConfig;
import com.aliyun.odps.kafka.connect.SinkStatusContext.Status;
import com.aliyun.odps.kafka.connect.converter.RecordConverter;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class BufferedWriterTest {
    private BufferedWriter writer(TableTunnel tunnel) {
        Map<String, String> props = new HashMap<>();
        props.put("endpoint", "http://example.invalid/api");
        props.put("project", "test"); props.put("table", "test");
        props.put("access_id", "test"); props.put("access_key", "test");
        props.put("time_zone", "UTC"); props.put("partition_window_type", "DAY");
        return new BufferedWriter(tunnel, new ConnectorConfig(props), "test", "test", mock(RecordConverter.class), null);
    }
    private TableTunnel tunnel(TableTunnel.StreamUploadSession session) throws Exception {
        TableTunnel tunnel = mock(TableTunnel.class, RETURNS_DEEP_STUBS);
        when(tunnel.buildStreamUploadSession("test", "test").setPartitionSpec(any(com.aliyun.odps.PartitionSpec.class)).setCreatePartition(true).build()).thenReturn(session);
        return tunnel;
    }
    @Test public void reusesSessionForSuccessiveBatchesAndPreservesMetrics() throws Exception {
        TableTunnel.StreamUploadSession session = mock(TableTunnel.StreamUploadSession.class);
        TableTunnel.StreamRecordPack pack = mock(TableTunnel.StreamRecordPack.class);
        when(session.newRecordPack(any(CompressOption.class))).thenReturn(pack);
        when(pack.getDataSize()).thenReturn(16L);
        BufferedWriter writer = writer(tunnel(session));
        for (int i = 0; i < 3; i++) {
            writer.write(new SinkRecord("topic", 0, null, null, null, "value", i));
            Status status = writer.flushAndReset();
            assertEquals(i, status.getMaxOffset());
            assertEquals(1, status.getProcessedRecords());
            assertEquals(16, status.getProcessedBytes());
        }
        verify(session, times(1)).newRecordPack(any(CompressOption.class));
        verify(pack, times(3)).flush();
        assertEquals(0, writer.flushAndReset().getProcessedRecords());
        verify(pack, times(3)).flush();
    }
    @Test public void createsNewPackWhenPartitionWindowChanges() throws Exception {
        TableTunnel.StreamUploadSession session = mock(TableTunnel.StreamUploadSession.class);
        when(session.newRecordPack(any(CompressOption.class))).thenReturn(mock(TableTunnel.StreamRecordPack.class));
        BufferedWriter writer = writer(tunnel(session));
        Method initialize = BufferedWriter.class.getDeclaredMethod("initStreamUploadSession", long.class);
        initialize.setAccessible(true);
        initialize.invoke(writer, 1700000000L);
        initialize.invoke(writer, 1700000060L);
        initialize.invoke(writer, 1700086400L);
        verify(session, times(2)).newRecordPack(any(CompressOption.class));
    }
    @Test public void failedFlushKeepsBatchForRetry() throws Exception {
        TableTunnel.StreamUploadSession session = mock(TableTunnel.StreamUploadSession.class);
        TableTunnel.StreamRecordPack pack = mock(TableTunnel.StreamRecordPack.class);
        when(session.newRecordPack(any(CompressOption.class))).thenReturn(pack);
        when(pack.flush()).thenThrow(new IOException("retry")).thenReturn("trace");
        BufferedWriter writer = writer(tunnel(session));
        writer.write(new SinkRecord("topic", 0, null, null, null, "value", 7));
        try { writer.flushAndReset(); fail("expected flush failure"); } catch (RuntimeException expected) { }
        assertEquals(1, writer.flushAndReset().getProcessedRecords());
        verify(session, times(1)).newRecordPack(any(CompressOption.class));
        verify(pack, times(2)).flush();
    }
    @Test public void failedWindowTransitionDoesNotReuseThePreviousPartition() throws Exception {
        TableTunnel.StreamUploadSession session = mock(TableTunnel.StreamUploadSession.class);
        when(session.newRecordPack(any(CompressOption.class))).thenReturn(mock(TableTunnel.StreamRecordPack.class));
        TableTunnel tunnel = tunnel(session);
        when(tunnel.buildStreamUploadSession("test", "test").setPartitionSpec(any(com.aliyun.odps.PartitionSpec.class))
            .setCreatePartition(true).build()).thenReturn(session).thenThrow(new RuntimeException("create failed")).thenReturn(session);
        BufferedWriter writer = writer(tunnel);
        Method initialize = BufferedWriter.class.getDeclaredMethod("initStreamUploadSession", long.class);
        initialize.setAccessible(true);
        initialize.invoke(writer, 1700000000L);
        try { initialize.invoke(writer, 1700086400L); fail("expected transition failure"); }
        catch (java.lang.reflect.InvocationTargetException expected) { assertEquals("create failed", expected.getCause().getMessage()); }
        initialize.invoke(writer, 1700086400L);
        verify(session, times(2)).newRecordPack(any(CompressOption.class));
    }
}
