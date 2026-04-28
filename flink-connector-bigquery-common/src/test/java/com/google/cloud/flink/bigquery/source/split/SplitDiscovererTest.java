package com.google.cloud.flink.bigquery.source.split;

import org.apache.flink.util.function.SerializableSupplier;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadStream;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import com.google.protobuf.Timestamp;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/** Tests for {@link SplitDiscoverer}. */
public class SplitDiscovererTest {

    @Test
    public void testDiscoverSplits() throws IOException {
        // Mocking setup
        BigQueryServices.StorageReadClient mockClient =
                Mockito.mock(BigQueryServices.StorageReadClient.class);
        ReadSession mockSession = Mockito.mock(ReadSession.class);
        ReadStream mockStream1 = Mockito.mock(ReadStream.class);
        ReadStream mockStream2 = Mockito.mock(ReadStream.class);

        when(mockStream1.getName()).thenReturn("projects/p/datasets/d/tables/t/streams/s1");
        when(mockStream2.getName()).thenReturn("projects/p/datasets/d/tables/t/streams/s2");
        when(mockSession.getStreamsList()).thenReturn(Arrays.asList(mockStream1, mockStream2));
        when(mockSession.getName()).thenReturn("projects/p/datasets/d/tables/t/sessions/sess1");
        when(mockSession.getExpireTime())
                .thenReturn(Timestamp.newBuilder().setSeconds(12345).build());
        when(mockClient.createReadSession(any())).thenReturn(mockSession);

        BigQueryServices mockServices = Mockito.mock(BigQueryServices.class);
        when(mockServices.createStorageReadClient(any())).thenReturn(mockClient);

        BigQueryServices.QueryDataClient mockQueryClient =
                Mockito.mock(BigQueryServices.QueryDataClient.class);
        BigQuery mockBigQuery = Mockito.mock(BigQuery.class);
        com.google.cloud.bigquery.Table mockTable =
                Mockito.mock(com.google.cloud.bigquery.Table.class);
        TableDefinition mockTableDefinition = Mockito.mock(TableDefinition.class);

        when(mockServices.createQueryDataClient(any())).thenReturn(mockQueryClient);
        when(mockQueryClient.getBigQuery()).thenReturn(mockBigQuery);
        when(mockBigQuery.getTable(any(TableId.class))).thenReturn(mockTable);
        when(mockTable.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(TableDefinition.Type.TABLE);

        SerializableSupplier<BigQueryServices> testingServices = () -> mockServices;

        BigQueryConnectOptions options =
                BigQueryConnectOptions.builder()
                        .setProjectId("p")
                        .setDataset("d")
                        .setTable("t")
                        .setTestingBigQueryServices(testingServices)
                        .build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        options,
                        DataFormat.AVRO,
                        Arrays.asList("col1"),
                        "restriction",
                        Optional.empty(),
                        -1,
                        1);

        assertThat(splits)
                .containsExactly(
                        "projects/p/datasets/d/tables/t/streams/s1",
                        "projects/p/datasets/d/tables/t/streams/s2");
    }

    @Test
    public void testDiscoverSplitsForMaterializedView() throws IOException, InterruptedException {
        // Mocking setup
        BigQueryServices.StorageReadClient mockClient =
                Mockito.mock(BigQueryServices.StorageReadClient.class);
        ReadSession mockSession = Mockito.mock(ReadSession.class);
        ReadStream mockStream1 = Mockito.mock(ReadStream.class);

        when(mockStream1.getName()).thenReturn("projects/p/datasets/d/tables/temp_view_123/streams/s1");
        when(mockSession.getStreamsList()).thenReturn(Arrays.asList(mockStream1));
        when(mockSession.getName()).thenReturn("projects/p/datasets/d/tables/temp_view_123/sessions/sess1");
        when(mockSession.getExpireTime())
                .thenReturn(Timestamp.newBuilder().setSeconds(12345).build());
        when(mockClient.createReadSession(any())).thenReturn(mockSession);

        BigQueryServices mockServices = Mockito.mock(BigQueryServices.class);
        when(mockServices.createStorageReadClient(any())).thenReturn(mockClient);

        BigQueryServices.QueryDataClient mockQueryClient =
                Mockito.mock(BigQueryServices.QueryDataClient.class);
        BigQuery mockBigQuery = Mockito.mock(BigQuery.class);
        com.google.cloud.bigquery.Table mockTable =
                Mockito.mock(com.google.cloud.bigquery.Table.class);
        TableDefinition mockTableDefinition = Mockito.mock(TableDefinition.class);

        when(mockServices.createQueryDataClient(any())).thenReturn(mockQueryClient);
        when(mockQueryClient.getBigQuery()).thenReturn(mockBigQuery);
        when(mockBigQuery.getTable(any(TableId.class))).thenReturn(mockTable);
        when(mockTable.getDefinition()).thenReturn(mockTableDefinition);
        when(mockTableDefinition.getType()).thenReturn(TableDefinition.Type.MATERIALIZED_VIEW);

        Job mockJob = Mockito.mock(Job.class);
        com.google.cloud.bigquery.JobStatus mockStatus = Mockito.mock(com.google.cloud.bigquery.JobStatus.class);
        when(mockBigQuery.create(any(JobInfo.class))).thenReturn(mockJob);
        when(mockJob.waitFor()).thenReturn(mockJob);
        when(mockJob.getStatus()).thenReturn(mockStatus);
        when(mockStatus.getError()).thenReturn(null);

        SerializableSupplier<BigQueryServices> testingServices = () -> mockServices;

        BigQueryConnectOptions options =
                BigQueryConnectOptions.builder()
                        .setProjectId("p")
                        .setDataset("d")
                        .setTable("t")
                        .setTestingBigQueryServices(testingServices)
                        .build();

        List<String> splits =
                SplitDiscoverer.discoverSplits(
                        options,
                        DataFormat.AVRO,
                        Arrays.asList("col1"),
                        "restriction",
                        Optional.empty(),
                        -1,
                        1);

        assertThat(splits)
                .containsExactly("projects/p/datasets/d/tables/temp_view_123/streams/s1");
    }
}
