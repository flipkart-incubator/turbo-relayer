/*
 *
 *  Copyright (c) 2022 [The original author]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package com.flipkart.varidhi.jobs;

import com.flipkart.varidhi.config.RelayerConfiguration;
import com.flipkart.varidhi.partitionManager.PartitionQueryModel;
import com.flipkart.varidhi.repository.TRPartitionQueriesLogger;
import lombok.NonNull;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class PartitionQueryFetcher implements TRPartitionQueriesLogger {

    private PartitionQueryModel partitionQueryModel;

    public PartitionQueryFetcher() {
        init();
    }

    @Override
    public void init() {
        partitionQueryModel = new PartitionQueryModel();
    }

    @Override
    public void collect(@NonNull String tableName, @NonNull RelayerConfiguration relayerConfiguration,
                        @NonNull DBTYPE dbtype, @NonNull String query) {

        if (DBTYPE.APPLICATION == dbtype) {
            Map<String, Set<String>> applicationDBMap = partitionQueryModel.getApplication();
            updateQueries(tableName, query, applicationDBMap);
            partitionQueryModel.setApplication(applicationDBMap);
        }
        if (DBTYPE.TR == dbtype) {
            Map<String, Set<String>> turboDBMap = partitionQueryModel.getTurbo();
            updateQueries(tableName, query, turboDBMap);
            partitionQueryModel.setTurbo(turboDBMap);
        }
    }

    @Override
    public void collectException(String exceptionMessage) {
        partitionQueryModel.getErrors().add(exceptionMessage);
    }

    private void updateQueries(String tableName, String query, Map<String, Set<String>> dBMap) {
        Set<String> queryList = dBMap.getOrDefault(tableName, new LinkedHashSet<>());
        queryList.add(query + ";");
        dBMap.put(tableName, queryList);
    }

    @Override
    public void finish() {

    }

    public PartitionQueryModel getPartitionQueryModel() {
        return partitionQueryModel;
    }
}
