/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.mist.core.task;

import edu.snu.mist.common.AdjacentListDAG;
import edu.snu.mist.common.DAG;
import edu.snu.mist.common.MistDataEvent;
import edu.snu.mist.common.sinks.Sink;
import edu.snu.mist.formats.avro.Direction;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Test;

import static org.mockito.Mockito.*;

public final class EventRouterTest {

  /**
   * Test whether EventRouter routes events correctly.
   * op5 -\
   * op1 -> op2 -> op3 -> sink1
   *            -> op4 -> sink2
   */
  @Test
  public void testEventRouting() throws InjectionException {
    final PhysicalOperator op1 = mock(PhysicalOperator.class);
    final PhysicalOperator op2 = mock(PhysicalOperator.class);
    final PhysicalOperator op3 = mock(PhysicalOperator.class);
    final PhysicalOperator op4 = mock(PhysicalOperator.class);
    final PhysicalOperator op5 = mock(PhysicalOperator.class);

    when(op1.getType()).thenReturn(PhysicalVertex.Type.OPERATOR);
    when(op2.getType()).thenReturn(PhysicalVertex.Type.OPERATOR);
    when(op3.getType()).thenReturn(PhysicalVertex.Type.OPERATOR);
    when(op4.getType()).thenReturn(PhysicalVertex.Type.OPERATOR);
    when(op5.getType()).thenReturn(PhysicalVertex.Type.OPERATOR);

    final PhysicalSink sink1 = mock(PhysicalSink.class);
    final Sink s1 = mock(Sink.class);
    when(sink1.getSink()).thenReturn(s1);
    when(sink1.getType()).thenReturn(PhysicalVertex.Type.SINK);

    final PhysicalSink sink2 = mock(PhysicalSink.class);
    final Sink s2 = mock(Sink.class);
    when(sink2.getSink()).thenReturn(s2);
    when(sink2.getType()).thenReturn(PhysicalVertex.Type.SINK);

    final DAG<PhysicalVertex, Direction> physicalDAG = new AdjacentListDAG<>();
    physicalDAG.addVertex(op1);
    physicalDAG.addVertex(op2);
    physicalDAG.addVertex(op3);
    physicalDAG.addVertex(op4);
    physicalDAG.addVertex(op5);
    physicalDAG.addVertex(sink1);
    physicalDAG.addVertex(sink2);

    physicalDAG.addEdge(op5, op2, Direction.RIGHT);
    physicalDAG.addEdge(op1, op2, Direction.LEFT);
    physicalDAG.addEdge(op2, op3, Direction.LEFT);
    physicalDAG.addEdge(op2, op4, Direction.LEFT);
    physicalDAG.addEdge(op3, sink1, Direction.LEFT);
    physicalDAG.addEdge(op4, sink2, Direction.LEFT);

    final InvertedVertexIndex invertedVertexIndex = mock(InvertedVertexIndex.class);
    when(invertedVertexIndex.read(op1)).thenReturn(physicalDAG);
    when(invertedVertexIndex.read(op2)).thenReturn(physicalDAG);
    when(invertedVertexIndex.read(op3)).thenReturn(physicalDAG);
    when(invertedVertexIndex.read(op4)).thenReturn(physicalDAG);
    when(invertedVertexIndex.read(op5)).thenReturn(physicalDAG);


    // Create an event router
    final Injector injector = Tang.Factory.getTang().newInjector();
    injector.bindVolatileInstance(InvertedVertexIndex.class, invertedVertexIndex);
    final EventRouter eventRouter = injector.getInstance(EventRouter.class);

    final MistDataEvent event = new MistDataEvent(1, 1L);

    // Check routing
    // op1 -> op2
    eventRouter.emitData(event, new EventContextImpl(op1));
    verify(invertedVertexIndex).read(op1);
    verify(op2).addOrProcessNextDataEvent(event, Direction.LEFT);
    // op5 -> op2
    eventRouter.emitData(event, new EventContextImpl(op5));
    verify(invertedVertexIndex).read(op5);
    verify(op2).addOrProcessNextDataEvent(event, Direction.RIGHT);
    // op2 -> op3
    //     -> op4
    eventRouter.emitData(event, new EventContextImpl(op2));
    verify(invertedVertexIndex).read(op2);
    verify(op3).addOrProcessNextDataEvent(event, Direction.LEFT);
    verify(op4).addOrProcessNextDataEvent(event, Direction.LEFT);
    // op3 -> sink1
    eventRouter.emitData(event, new EventContextImpl(op3));
    verify(invertedVertexIndex).read(op3);
    verify(sink1).getSink();
    // op4 -> sink2
    eventRouter.emitData(event, new EventContextImpl(op4));
    verify(invertedVertexIndex).read(op4);
    verify(sink2).getSink();
  }
}
