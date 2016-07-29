/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.api.serialize.avro;

import edu.snu.mist.api.*;
import edu.snu.mist.api.operators.*;
import edu.snu.mist.api.serialize.avro.params.RunningJarPath;
import edu.snu.mist.api.sink.Sink;
import edu.snu.mist.api.sources.SourceStream;
import edu.snu.mist.formats.avro.Edge;
import edu.snu.mist.formats.avro.LogicalPlan;
import edu.snu.mist.formats.avro.Vertex;
import edu.snu.mist.formats.avro.VertexTypeEnum;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The implementation class for MISTQuerySerializer interface.
 */
public final class MISTQuerySerializerImpl implements MISTQuerySerializer {

  private final SourceInfoProvider sourceInfoProvider;
  private final SinkInfoProvider sinkInfoProvider;
  private final WindowOperatorInfoProvider windowOperatorInfoProvider;
  private final InstantOperatorInfoProvider instantOperatorInfoProvider;
  private final String runningJarPathString;

  @Inject
  private MISTQuerySerializerImpl(
      @Parameter(RunningJarPath.class) final String runningJarPath) throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.sourceInfoProvider = injector.getInstance(SourceInfoProvider.class);
    this.sinkInfoProvider = injector.getInstance(SinkInfoProvider.class);
    this.windowOperatorInfoProvider = injector.getInstance(WindowOperatorInfoProvider.class);
    this.instantOperatorInfoProvider = injector.getInstance(InstantOperatorInfoProvider.class);
    if (runningJarPath.endsWith(".jar")) {
      this.runningJarPathString = runningJarPath;
    } else {
      throw new IllegalArgumentException("Client running jar path should end with '.jar'");
    }
  }

  @Inject
  private MISTQuerySerializerImpl() throws InjectionException {
    final Injector injector = Tang.Factory.getTang().newInjector();
    this.sourceInfoProvider = injector.getInstance(SourceInfoProvider.class);
    this.sinkInfoProvider = injector.getInstance(SinkInfoProvider.class);
    this.windowOperatorInfoProvider = injector.getInstance(WindowOperatorInfoProvider.class);
    this.instantOperatorInfoProvider = injector.getInstance(InstantOperatorInfoProvider.class);
    this.runningJarPathString = null;
  }

  private Vertex buildAvroVertex(final Object apiVertex) {
    Vertex.Builder vertexBuilder = Vertex.newBuilder();
    if (apiVertex instanceof Sink) {
      vertexBuilder.setVertexType(VertexTypeEnum.SINK);
      vertexBuilder.setAttributes(sinkInfoProvider.getSinkInfo((Sink) apiVertex));
    } else if (apiVertex instanceof SourceStream) {
      vertexBuilder.setVertexType(VertexTypeEnum.SOURCE);
      vertexBuilder.setAttributes(sourceInfoProvider.getSourceInfo((SourceStream) apiVertex));
    } else if (apiVertex instanceof InstantOperatorStream) {
      vertexBuilder.setVertexType(VertexTypeEnum.INSTANT_OPERATOR);
      vertexBuilder.setAttributes(
          instantOperatorInfoProvider.getInstantOperatorInfo((InstantOperatorStream) apiVertex));
    } else if (apiVertex instanceof WindowedStream) {
      vertexBuilder.setVertexType(VertexTypeEnum.WINDOW_OPERATOR);
      vertexBuilder.setAttributes(windowOperatorInfoProvider.getWindowOperatorInfo((WindowedStream) apiVertex));
    } else {
      throw new IllegalStateException("apiVertex type is illegal!");
    }
    return vertexBuilder.build();
  }

  @Override
  public LogicalPlan queryToLogicalPlan(final MISTQuery query) throws IOException, URISyntaxException {
    final List<Object> apiVertices = new ArrayList<>();
    final List<Edge> edges = new ArrayList<>();
    final Queue<Object> queue = new LinkedList<>();
    final byte[] runningJarBytes;
    final boolean isJarSerialized;
    if (runningJarPathString == null) {
      runningJarBytes = new byte[1];
      runningJarBytes[0] = 0;
      isJarSerialized = false;
    } else {
      // Serialize running JAR file first
      final File runningJarFile = new File(runningJarPathString);
      final Path runningJarPath = runningJarFile.toPath();
      runningJarBytes = Files.readAllBytes(runningJarPath);
      isJarSerialized = true;
    }
    // Traverse queries in BFS order
    apiVertices.addAll(query.getQuerySinks());
    queue.addAll(query.getQuerySinks());
    while(!queue.isEmpty()) {
      final Object apiVertex = queue.remove();
      final int toIndex = apiVertices.indexOf(apiVertex);
      final Set<MISTStream> precedingStreams;
      if (apiVertex instanceof Sink) {
        final Sink sink = (Sink) apiVertex;
        precedingStreams = sink.getPrecedingStreams();
      } else if (apiVertex instanceof MISTStream) {
        MISTStream stream = (MISTStream) apiVertex;
        precedingStreams = stream.getInputStreams();
      } else {
        throw new IllegalStateException("apiVertex is neither Sink nor MISTStream!");
      }
      if (precedingStreams != null) {
        AtomicBoolean isLeftEdge = new AtomicBoolean(true);

        for (MISTStream precedingStream : precedingStreams) {
          if (!apiVertices.contains(precedingStream)) {
            apiVertices.add(precedingStream);
            queue.add(precedingStream);
          }
          final int fromIndex = apiVertices.indexOf(precedingStream);
          Edge.Builder newEdgeBuilder = Edge.newBuilder()
              .setFrom(fromIndex)
              .setTo(toIndex);

          if (isLeftEdge.get()) {
            Edge newEdge = newEdgeBuilder.setIsLeft(true)
                .build();
            isLeftEdge.set(false);
            edges.add(newEdge);
          } else {
            Edge newEdge = newEdgeBuilder.setIsLeft(false)
                .build();
            edges.add(newEdge);
          }
        }
      }
    }
    // Serialize each apiVertices via avro.
    final List<Vertex> serializedVertices = new ArrayList<>();
    for (Object apiVertex : apiVertices) {
      serializedVertices.add(buildAvroVertex(apiVertex));
    }
    // Build logical plan using serialized vertices and edges.
    final LogicalPlan.Builder logicalPlanBuilder = LogicalPlan.newBuilder();
    final LogicalPlan logicalPlan = logicalPlanBuilder
        .setIsJarSerialized(isJarSerialized)
        .setJar(ByteBuffer.wrap(runningJarBytes))
        .setVertices(serializedVertices)
        .setEdges(edges)
        .build();
    return logicalPlan;
  }
}