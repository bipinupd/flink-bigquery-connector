/*
 * Copyright (C) 2023 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.flink.bigquery.source.split;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.ViewDefinition;
import com.google.cloud.bigquery.connector.common.BigQueryClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.flink.bigquery.common.config.BigQueryConnectOptions;
import com.google.cloud.flink.bigquery.services.BigQueryServices;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link SplitDiscoverer}. */
public class SplitDiscovererTest {

    private BigQueryServices mockServices;
    private BigQueryServices.StorageReadClient mockStorageClient;
    private BigQueryServices.QueryDataClient mockQueryClient;
    private BigQuery mockBigQuery;

    @Before
    public void setUp() throws IOException {
        mockServices = mock(BigQueryServices.class);
        mockStorageClient = mock(BigQueryServices.StorageReadClient.class);
        mockQueryClient = mock(BigQueryServices.QueryDataClient.class);
        mockBigQuery = mock(BigQuery.class);

        when(mockServices.createStorageReadClient(any())).thenReturn(mockStorageClient);
        when(mockServices.createQueryDataClient(any())).thenReturn(mockQueryClient);
        when(mockQueryClient.getBigQuery()).thenReturn(mockBigQuery);
    }

    @Test
    public void testDiscoverSplitsForView() throws IOException {
        BigQueryConnectOptions options =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("view")
                        .setTestingBigQueryServices(() -> mockServices)
                        .build();

        TableId viewId = TableId.of("project", "dataset", "view");
        Table mockTable = mock(Table.class);
        ViewDefinition mockViewDef = mock(ViewDefinition.class);

        when(mockBigQuery.getTable(viewId)).thenReturn(mockTable);
        when(mockTable.getDefinition()).thenReturn(mockViewDef);
        when(mockViewDef.getType()).thenReturn(TableDefinition.Type.VIEW);
        when(mockViewDef.getQuery()).thenReturn("SELECT * FROM table");

        TableId tempTableId = TableId.of("project", "temp_dataset", "temp_table");
        TableInfo materializedTableInfo = TableInfo.of(tempTableId, mock(TableDefinition.class));

        ReadSession mockSession = mock(ReadSession.class);
        when(mockStorageClient.createReadSession(any(CreateReadSessionRequest.class)))
                .thenReturn(mockSession);
        when(mockSession.getStreamsList()).thenReturn(new ArrayList<>());
        when(mockSession.getExpireTime())
                .thenReturn(com.google.protobuf.Timestamp.newBuilder().setSeconds(100).build());

        try (MockedConstruction<BigQueryClient> mocked =
                Mockito.mockConstruction(
                        BigQueryClient.class,
                        (mock, context) -> {
                            when(mock.materializeViewToTable(
                                            anyString(), any(TableId.class), anyInt()))
                                    .thenReturn(materializedTableInfo);
                        })) {

            List<String> splits =
                    SplitDiscoverer.discoverSplits(
                            options,
                            DataFormat.AVRO,
                            Arrays.asList("col1"),
                            "",
                            Optional.empty(),
                            -1,
                            -1);

            assertThat(splits).isEmpty();
            assertThat(mocked.constructed()).hasSize(1);
        }
    }

    @Test
    public void testDiscoverSplitsForTable() throws IOException {
        BigQueryConnectOptions options =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("table")
                        .setTestingBigQueryServices(() -> mockServices)
                        .build();

        TableId tableId = TableId.of("project", "dataset", "table");
        Table mockTable = mock(Table.class);
        TableDefinition mockTableDef = mock(TableDefinition.class);

        when(mockBigQuery.getTable(tableId)).thenReturn(mockTable);
        when(mockTable.getDefinition()).thenReturn(mockTableDef);
        when(mockTableDef.getType()).thenReturn(TableDefinition.Type.TABLE);

        ReadSession mockSession = mock(ReadSession.class);
        when(mockStorageClient.createReadSession(any(CreateReadSessionRequest.class)))
                .thenReturn(mockSession);
        when(mockSession.getStreamsList()).thenReturn(new ArrayList<>());
        when(mockSession.getExpireTime())
                .thenReturn(com.google.protobuf.Timestamp.newBuilder().setSeconds(100).build());

        try (MockedConstruction<BigQueryClient> mocked =
                Mockito.mockConstruction(BigQueryClient.class)) {

            List<String> splits =
                    SplitDiscoverer.discoverSplits(
                            options,
                            DataFormat.AVRO,
                            Arrays.asList("col1"),
                            "",
                            Optional.empty(),
                            -1,
                            -1);

            assertThat(splits).isEmpty();
            assertThat(mocked.constructed()).isEmpty();
        }
    }

    @Test
    public void testDiscoverSplitsForNonExistentTable() throws IOException {
        BigQueryConnectOptions options =
                BigQueryConnectOptions.builder()
                        .setProjectId("project")
                        .setDataset("dataset")
                        .setTable("non_existent")
                        .setTestingBigQueryServices(() -> mockServices)
                        .build();

        TableId tableId = TableId.of("project", "dataset", "non_existent");

        when(mockBigQuery.getTable(tableId)).thenReturn(null);

        ReadSession mockSession = mock(ReadSession.class);
        when(mockStorageClient.createReadSession(any(CreateReadSessionRequest.class)))
                .thenReturn(mockSession);
        when(mockSession.getStreamsList()).thenReturn(new ArrayList<>());
        when(mockSession.getExpireTime())
                .thenReturn(com.google.protobuf.Timestamp.newBuilder().setSeconds(100).build());

        try (MockedConstruction<BigQueryClient> mocked =
                Mockito.mockConstruction(BigQueryClient.class)) {

            List<String> splits =
                    SplitDiscoverer.discoverSplits(
                            options,
                            DataFormat.AVRO,
                            Arrays.asList("col1"),
                            "",
                            Optional.empty(),
                            -1,
                            -1);

            assertThat(splits).isEmpty();
            assertThat(mocked.constructed()).isEmpty();
        }
    }
}
