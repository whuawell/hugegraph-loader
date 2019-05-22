/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.loader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.loader.builder.EdgeBuilder;
import com.baidu.hugegraph.loader.builder.VertexBuilder;
import com.baidu.hugegraph.loader.constant.Constants;
import com.baidu.hugegraph.loader.constant.ElemType;
import com.baidu.hugegraph.loader.exception.LoadException;
import com.baidu.hugegraph.loader.exception.ParseException;
import com.baidu.hugegraph.loader.executor.FailureLogger;
import com.baidu.hugegraph.loader.executor.GroovyExecutor;
import com.baidu.hugegraph.loader.executor.LoadOptions;
import com.baidu.hugegraph.loader.progress.InputProgressMap;
import com.baidu.hugegraph.loader.progress.LoadProgress;
import com.baidu.hugegraph.loader.source.graph.EdgeSource;
import com.baidu.hugegraph.loader.source.graph.GraphSource;
import com.baidu.hugegraph.loader.source.graph.VertexSource;
import com.baidu.hugegraph.loader.summary.LoadMetrics;
import com.baidu.hugegraph.loader.summary.LoadSummary;
import com.baidu.hugegraph.loader.task.TaskManager;
import com.baidu.hugegraph.loader.util.HugeClientWrapper;
import com.baidu.hugegraph.loader.util.LoadUtil;
import com.baidu.hugegraph.loader.util.Printer;
import com.baidu.hugegraph.structure.graph.Edge;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.util.E;
import com.baidu.hugegraph.util.Log;
import com.beust.jcommander.JCommander;

public final class HugeGraphLoader {

    private static final Logger LOG = Log.logger(HugeGraphLoader.class);
    private static final FailureLogger LOG_PARSE =
                         FailureLogger.logger("parseError");

    private final LoadOptions options;
    private final GraphSource graphSource;
    // The old progress just used to read
    private final LoadProgress oldProgress;
    private final LoadProgress newProgress;
    private final LoadSummary loadSummary;
    private final TaskManager taskManager;

    public static void main(String[] args) {
        HugeGraphLoader loader = new HugeGraphLoader(args);
        loader.load();
    }

    private HugeGraphLoader(String[] args) {
        this.options = parseAndCheckOptions(args);
        this.graphSource = GraphSource.of(this.options.file);
        this.oldProgress = parseLoadProgress(this.options);
        this.newProgress = new LoadProgress();
        this.loadSummary = new LoadSummary();
        this.taskManager = new TaskManager(this.options, this.loadSummary);
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                this.newProgress.write(this.options.file);
            } catch (IOException e) {
                LOG.warn("Failed to write load progress", e);
            }
        }));
    }

    private static LoadOptions parseAndCheckOptions(String[] args) {
        LoadOptions options = new LoadOptions();
        JCommander commander = JCommander.newBuilder()
                                         .addObject(options)
                                         .build();
        commander.parse(args);
        // Print usage and exit
        if (options.help) {
            LoadUtil.exitWithUsage(commander, Constants.EXIT_CODE_NORM);
        }
        // Check options
        // Check option "-f"
        E.checkArgument(!StringUtils.isEmpty(options.file),
                        "Must specified entrance groovy file");
        File scriptFile = new File(options.file);
        if (!scriptFile.canRead()) {
            LOG.error("Script file must be readable: '{}'",
                      scriptFile.getAbsolutePath());
            LoadUtil.exitWithUsage(commander, Constants.EXIT_CODE_ERROR);
        }
        // Check option "-g"
        E.checkArgument(!StringUtils.isEmpty(options.graph),
                        "Must specified a graph");
        // Check option "-h"
        String httpPrefix = "http://";
        if (!options.host.startsWith(httpPrefix)) {
            options.host = httpPrefix + options.host;
        }
        return options;
    }

    private static LoadProgress parseLoadProgress(LoadOptions options) {
        if (!options.incrementalMode) {
            return new LoadProgress();
        }
        try {
            return LoadProgress.read(options.file);
        } catch (IOException e) {
            throw new LoadException("Failed to read progress file", e);
        }
    }

    private void load() {
        // Create schema
        this.createSchema();

        // Load vertices
        this.loadVertices(this.loadSummary.vertexMetrics());
        // Load edges
        this.loadEdges(this.loadSummary.edgeMetrics());
        // Print load summary
        Printer.printSummary(this.loadSummary);

        this.stopLoading(Constants.EXIT_CODE_NORM);
    }

    private void createSchema() {
        File schemaFile = FileUtils.getFile(this.options.schema);
        HugeClient client = HugeClientWrapper.get(this.options);
        GroovyExecutor groovyExecutor = new GroovyExecutor();
        groovyExecutor.bind("schema", client.schema());
        String script;
        try {
            script = FileUtils.readFileToString(schemaFile, Constants.CHARSET);
        } catch (IOException e) {
            throw new LoadException("Read schema file '%s' error",
                                    e, this.options.schema);
        }
        groovyExecutor.execute(script, client);
    }

    private void loadVertices(LoadMetrics metrics) {
        Printer.printElemType(metrics.type());
        metrics.startTimer();
        // Used to get and update progress
        InputProgressMap oldVertexProgress = this.oldProgress.vertex();
        InputProgressMap newVertexProgress = this.newProgress.vertex();
        // Execute loading tasks
        List<VertexSource> vertexSources = this.graphSource.vertexSources();
        for (VertexSource source : vertexSources) {
            // Update loading element source
            newVertexProgress.addSource(source);
            LOG.info("Loading vertex source '{}'", source.label());
            try (VertexBuilder builder = new VertexBuilder(source,
                                                           this.options)) {
                builder.progress(oldVertexProgress, newVertexProgress);
                this.loadVertex(builder, metrics);
            }
        }
        // Waiting async worker threads finish
        this.taskManager.waitFinished(ElemType.VERTEX);
        metrics.stopTimer();
        Printer.print(metrics.insertSuccess());
    }

    private void loadVertex(VertexBuilder builder, LoadMetrics metrics) {
        int batchSize = this.options.batchSize;
        List<Vertex> batch = new ArrayList<>(batchSize);
        while (builder.hasNext()) {
            try {
                Vertex vertex = builder.next();
                batch.add(vertex);
            } catch (ParseException e) {
                if (this.options.testMode) {
                    throw e;
                }
                LOG.error("Vertex parse error", e);
                LOG_PARSE.error(e);
                long failureNum = metrics.increaseParseFailure();
                if (failureNum >= this.options.maxParseErrors) {
                    Printer.printError("Error: More than %s vertices " +
                                       "parsing error ... stopping",
                                       this.options.maxParseErrors);
                    this.stopLoading(Constants.EXIT_CODE_ERROR);
                }
                continue;
            }
            if (batch.size() >= batchSize) {
                this.taskManager.submitVertexBatch(batch);
                batch = new ArrayList<>(batchSize);
            }
        }
        if (!batch.isEmpty()) {
            this.taskManager.submitVertexBatch(batch);
        }
    }

    private void loadEdges(LoadMetrics metrics) {
        Printer.printElemType(metrics.type());
        metrics.startTimer();
        // Used to get and update progress
        InputProgressMap oldEdgeProgress = this.oldProgress.edge();
        InputProgressMap newEdgeProgress = this.newProgress.edge();
        // Execute loading tasks
        List<EdgeSource> edgeSources = this.graphSource.edgeSources();
        for (EdgeSource source : edgeSources) {
            // Update loading element source
            newEdgeProgress.addSource(source);
            LOG.info("Loading edge source '{}'", source.label());
            try (EdgeBuilder builder = new EdgeBuilder(source, this.options)) {
                builder.progress(oldEdgeProgress, newEdgeProgress);
                this.loadEdge(builder, metrics);
            }
        }
        // Waiting async worker threads finish
        this.taskManager.waitFinished(ElemType.EDGE);
        metrics.stopTimer();
        Printer.print(metrics.insertSuccess());
    }

    private void loadEdge(EdgeBuilder builder, LoadMetrics metrics) {
        int batchSize = this.options.batchSize;
        List<Edge> batch = new ArrayList<>(batchSize);
        while (builder.hasNext()) {
            try {
                Edge edge = builder.next();
                batch.add(edge);
            } catch (ParseException e) {
                if (this.options.testMode) {
                    throw e;
                }
                LOG.error("Edge parse error", e);
                LOG_PARSE.error(e);
                long failureNum = metrics.increaseParseFailure();
                if (failureNum >= this.options.maxParseErrors) {
                    Printer.printError("Error: More than %s edges " +
                                       "parsing error ... stopping",
                                       this.options.maxParseErrors);
                    this.stopLoading(Constants.EXIT_CODE_ERROR);
                }
                continue;
            }
            if (batch.size() >= batchSize) {
                this.taskManager.submitEdgeBatch(batch);
                batch = new ArrayList<>(batchSize);
            }
        }
        if (!batch.isEmpty()) {
            this.taskManager.submitEdgeBatch(batch);
        }
    }

    private void stopLoading(int code) {
        // Shutdown task manager
        this.taskManager.shutdown(this.options.shutdownTimeout);
        // Exit JVM if the code is not EXIT_CODE_NORM
        if (Constants.EXIT_CODE_NORM != code) {
            LoadUtil.exit(code);
        }
    }
}
