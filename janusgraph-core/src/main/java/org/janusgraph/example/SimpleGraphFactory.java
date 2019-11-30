// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.example;

import com.google.common.base.Preconditions;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class SimpleGraphFactory {

 public static final String INDEX_NAME = "search";
 private static final String ERR_NO_INDEXING_BACKEND = 
         "The indexing backend with name \"%s\" is not defined. Specify an existing indexing backend or " +
         "use GraphOfTheGodsFactory.loadWithoutMixedIndex(graph,true) to load without the use of an " +
         "indexing backend.";

 public static JanusGraph create(final String directory) {
     JanusGraphFactory.Builder config = JanusGraphFactory.build();
     config.set("storage.backend", "berkeleyje");
     config.set("storage.directory", directory);
     // config.set("index." + INDEX_NAME + ".backend", "elasticsearch");

     JanusGraph graph = config.open();
     SimpleGraphFactory.load(graph);
     return graph;
 }

 public static void loadWithoutMixedIndex(final JanusGraph graph, boolean uniqueNameCompositeIndex) {
     load(graph, null, uniqueNameCompositeIndex);
 }

 public static void load(final JanusGraph graph) {
     load(graph, INDEX_NAME, true);
 }

 private static boolean mixedIndexNullOrExists(StandardJanusGraph graph, String indexName) {
     return indexName == null || graph.getIndexSerializer().containsIndex(indexName);
 }

 public static void load(final JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {
     if (graph instanceof StandardJanusGraph) {
         Preconditions.checkState(mixedIndexNullOrExists((StandardJanusGraph)graph, mixedIndexName), 
                 ERR_NO_INDEXING_BACKEND, mixedIndexName);
     }

     //Create Schema
     long neighbour_vids[] = new long[8];
     Vertex neighbour_vetices[] = new Vertex[8];

     JanusGraphManagement management = graph.openManagement();
     management.makeEdgeLabel("Edge_A").make();
     management.makeEdgeLabel("Edge_B").make();
     management.makeVertexLabel("Vertex_A").make();
     management.makeVertexLabel("Vertex_B").make();
     management.makeVertexLabel("Vertex_C").make();

     final PropertyKey vertex_property = management.makePropertyKey("VerTexPrOp").dataType(String.class).make();
     final PropertyKey edge_property = management.makePropertyKey("EDgepRoP").dataType(String.class).make();
     final PropertyKey vertex_property2 = management.makePropertyKey("VerTexPrOp2").dataType(String.class).make();
     final PropertyKey edge_property2 = management.makePropertyKey("EDgepRoP2").dataType(String.class).make();
     final PropertyKey vertex_property3 = management.makePropertyKey("VerTexPrOp3").dataType(String.class).make();
     final PropertyKey edge_property3 = management.makePropertyKey("EDgepRoP3").dataType(String.class).make();

     for(int i = 0 ; i< neighbour_vids.length; i++){
         long raw_id = i + 3;
         management.makeVertexLabel("Vertex_" + raw_id).make();
         //neighbour_vids[i] = ((StandardJanusGraph) graph).getIDManager().toVertexId(raw_id);
         //neighbour_vetices[i] = tx.addVertex(T.id, neighbour_vids[i], T.label, "Vertex_" + raw_id);
     }
     management.commit();

     JanusGraphTransaction tx = graph.newTransaction();

     long vidA_raw = 1L;
     long vidB_raw = 2L;
     long vidA = ((StandardJanusGraph) graph).getIDManager().toVertexId(vidA_raw);
     long vidB = ((StandardJanusGraph) graph).getIDManager().toVertexId(vidB_raw);
     Vertex vertexA = tx.addVertex(T.id, vidA, T.label, "Vertex_A", "VerTexPrOp", "vertexA_property", "VerTexPrOp2", "vertexA_property2", "VerTexPrOp3", "vertexA_property3" );
     Vertex vertexB = tx.addVertex(T.id, vidB, T.label, "Vertex_B",  "VerTexPrOp", "vertexB_property", "VerTexPrOp2", "vertexB_property2", "VerTexPrOp3", "vertexB_property3");


     for(int i = 0 ; i< neighbour_vids.length; i++){
         long raw_id = i + 3;
         neighbour_vids[i] = ((StandardJanusGraph) graph).getIDManager().toVertexId(raw_id);
         //neighbour_vetices[i] = tx.addVertex(T.id, neighbour_vids[i], T.label, "Vertex_" + raw_id);
         if (i%2==0){
             neighbour_vetices[i] = tx.addVertex(T.id, neighbour_vids[i], T.label, "Vertex_" + raw_id, "VerTexPrOp", "vprop_even");
         }
         else{
             neighbour_vetices[i] = tx.addVertex(T.id, neighbour_vids[i], T.label, "Vertex_" + raw_id);
         }
     }

     for(int i = 0; i < 2; i++){
         vertexA.addEdge("Edge_A", neighbour_vetices[i]);
         vertexA.addEdge("Edge_B", neighbour_vetices[i+2]);
         //vertexB.addEdge("Edge_A", neighbour_vetices[i+20]);
         neighbour_vetices[i+4].addEdge("Edge_A", vertexA, "EDgepRoP", "va_edge_a_eprop", "EDgepRoP2", "va_edge_a_eprop_2", "EDgepRoP3", "va_edge_a_eprop_3");
         //vertexB.addEdge("Edge_B", neighbour_vetices[i+30]);
         neighbour_vetices[i+6].addEdge("Edge_B", vertexA);
     }

     for(int i = 0; i < 2; i++){
         vertexB.addEdge("Edge_A", neighbour_vetices[i]);
         vertexB.addEdge("Edge_B", neighbour_vetices[i+2]);
         //vertexB.addEdge("Edge_A", neighbour_vetices[i+20]);
         neighbour_vetices[i+4].addEdge("Edge_A", vertexB);
         //vertexB.addEdge("Edge_B", neighbour_vetices[i+30]);
         neighbour_vetices[i+6].addEdge("Edge_B", vertexB, "EDgepRoP", "vb_edge_b_eprop", "EDgepRoP2", "vb_edge_b_eprop_2", "EDgepRoP3", "vb_edge_b_eprop_3");
     }
     /*
     long vid1 = 1L; //(long)v1.id();
     long vid2 = 2L;//(long)v2.id();
     long vid3 = 3L; //(long)v3.id();
     long vid4 = 4L; //(long)v1.id();
     long vid5 = 5L;//(long)v2.id();
     long vertexId1 = ((StandardJanusGraph) graph).getIDManager().toVertexId(vid1);
     long vertexId2 = ((StandardJanusGraph) graph).getIDManager().toVertexId(vid2);
     long vertexId3 = ((StandardJanusGraph) graph).getIDManager().toVertexId(vid3);
     long vertexId4 = ((StandardJanusGraph) graph).getIDManager().toVertexId(vid4);
     long vertexId5 = ((StandardJanusGraph) graph).getIDManager().toVertexId(vid5);
     System.out.println("vid 1: " + vertexId1);
     System.out.println("vid 2: " + vertexId2);
     System.out.println("vid 3: " + vertexId3);
     System.out.println("vid 4: " + vertexId4);
     System.out.println("vid 5: " + vertexId5);
     // vertices
     Vertex v1 = tx.addVertex(T.id, vertexId1, T.label, "Vertex_A");
     Vertex v2 = tx.addVertex(T.id, vertexId2, T.label, "Vertex_B");
     Vertex v3 = tx.addVertex(T.id, vertexId3, T.label, "Vertex_C");
     Vertex v4 = tx.addVertex(T.id, vertexId4, T.label, "Vertex_D");
     Vertex v5 = tx.addVertex(T.id, vertexId5, T.label, "Vertex_E");

     
     // edges
     v1.addEdge("Edge_A", v2);
     v1.addEdge("Edge_B", v3);
     v2.addEdge("Edge_B", v3);
     v3.addEdge("Edge_B", v2);
     v2.addEdge("Edge_A", v4);
     v4.addEdge("Edge_B", v5);

      */

     // commit the transaction to disk
     tx.commit();
 }

 /**
  * Calls {@link JanusGraphFactory#open(String)}, passing the JanusGraph configuration file path
  * which must be the sole element in the {@code args} array, then calls
  * {@link #load(org.janusgraph.core.JanusGraph)} on the opened graph,
  * then calls {@link org.janusgraph.core.JanusGraph#close()}
  * and returns.
  * <p>
  * This method may call {@link System#exit(int)} if it encounters an error, such as
  * failure to parse its arguments.  Only use this method when executing main from
  * a command line.  Use one of the other methods on this class ({@link #create(String)}
  * or {@link #load(org.janusgraph.core.JanusGraph)}) when calling from
  * an enclosing application.
  *
  * @param args a singleton array containing a path to a JanusGraph config properties file
  */
 public static void main(String args[]) {
     if (null == args || 1 != args.length) {
         System.err.println("Usage: SimpleGraphFactory <janusgraph-config-file>");
         System.exit(1);
     }

     JanusGraph g = JanusGraphFactory.open(args[0]);
     load(g);
     g.close();
 }
}

