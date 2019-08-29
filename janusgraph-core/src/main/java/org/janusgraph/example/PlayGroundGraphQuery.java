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
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

public class PlayGroundGraphQuery {

    public static final String INDEX_NAME = "search";
    public static final String DATA_PATH = "/home/lexu/codelab/ldbc_1w/";

    private static final String ERR_NO_INDEXING_BACKEND =
        "The indexing backend with name \"%s\" is not defined. Specify an existing indexing backend or " +
            "use GraphOfTheGodsFactory.loadWithoutMixedIndex(graph,true) to load without the use of an " +
            "indexing backend.";

    private static boolean mixedIndexNullOrExists(StandardJanusGraph graph, String indexName) {
        return indexName == null || graph.getIndexSerializer().containsIndex(indexName);
    }

    public static void load(final JanusGraph graph) {
        load(graph, INDEX_NAME, false);
    }

    public static void loadWithoutMixedIndex(final JanusGraph graph, boolean uniqueNameCompositeIndex) {
        load(graph, null, uniqueNameCompositeIndex);
    }

    public static void load(final JanusGraph graph, String mixedIndexName, boolean uniqueNameCompositeIndex) {
        if (graph instanceof StandardJanusGraph) {
            Preconditions.checkState(mixedIndexNullOrExists((StandardJanusGraph)graph, mixedIndexName),
                ERR_NO_INDEXING_BACKEND, mixedIndexName);
        }

        //Create Schema
        JanusGraphManagement management = graph.openManagement();
//        final PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
//        JanusGraphManagement.IndexBuilder nameIndexBuilder = management.buildIndex("name", Vertex.class).addKey(name);
//        if (uniqueNameCompositeIndex)
//            nameIndexBuilder.unique();
//        JanusGraphIndex nameIndex = nameIndexBuilder.buildCompositeIndex();
//        management.setConsistency(nameIndex, ConsistencyModifier.LOCK);
//        if (null != mixedIndexName)
//            management.buildIndex("vertices", Vertex.class).buildMixedIndex(mixedIndexName);
//        if (null != mixedIndexName)
//            management.buildIndex("edges", Edge.class).buildMixedIndex(mixedIndexName);

        String[] edgeLabels = {
            "hasCreator",
            "hasTag",
            "isLocatedIn",
            "replyOf",
            "containerOf",
            "hasMember",
            "hasModerator",
            "email",
            "hasInterest",
            "knows",
            "likes",
            "speaks",
            "studyAt",
            "workAt",
            "isPartOf",
            "hasType",
            "isSubclassOf"
        };
        String[] vertexLabels = {
//            "comment",
//            "forum",
//            "organisation",
            "person"
//            "place",
//            "post",
//            "tag",
//            "tagclass"
        };

        String[][] edgeFiles = {
//            {"comment", "hasCreator", "person"},
//            {"comment", "hasTag", "tag"},
//            {"comment", "isLocatedIn", "place"},
//            {"comment", "replyOf", "comment"},
//            {"comment", "replyOf", "post"},
//            {"forum", "containerOf", "post"},
//            {"forum", "hasMember", "person"},
//            {"forum", "hasModerator", "person"},
//            {"forum", "hasTag", "tag"},
//            {"organisation", "isLocatedIn", "place"},
//            {"person", "email", "emailaddress"},
//            {"person", "hasInterest", "tag"},
//            {"person", "isLocatedIn", "place"},
            {"person", "knows", "person"}
//            {"person", "likes", "comment"},
//            {"person", "likes", "post"},
//            {"person", "speaks", "language"},
//            {"person", "studyAt", "organisation"},
//            {"person", "workAt", "organisation"},
//            {"place", "isPartOf", "place"},
//            {"post", "hasCreator", "person"},
//            {"post", "hasTag", "tag"},
//            {"post", "isLocatedIn", "place"},
//            {"tag", "hasType", "tagclass"},
//            {"tagclass", "isSubclassOf", "tagclass"}
        };

        for(String vertex : vertexLabels){
            management.makeVertexLabel(vertex).make();
        }
        for(String edge: edgeLabels){
            management.makeEdgeLabel(edge).make();
        }

        management.commit();

        JanusGraphTransaction tx = graph.newTransaction();
        // vertices
        // Vertex saturn = tx.addVertex(T.label, "titan", "name", "saturn", "age", 10000);
        //create vertex
        HashMap<String, HashMap<String, Vertex>> vertices = new HashMap<>();
        for (String label : vertexLabels){
            String filename = label + "_0_0.csv";
            BufferedReader br = null;
            String line = "";
            String cvsSplitBy = "\n";
            int lineCount = 0;
            try {
                br = new BufferedReader(new FileReader(DATA_PATH + "vertex/"+ filename));
                while ((line = br.readLine()) != null) {
                    String[] arr = line.split(cvsSplitBy);
                    String id = arr[0];
                    if(!vertices.containsKey(label)){
                        vertices.put(label, new HashMap<>());
                    }
                    try{
//                        vertices.get(label).put(id, tx.addVertex(T.id, Long.parseLong(id), T.label, label, "name", id));
                        vertices.get(label).put(id, tx.addVertex( T.label, label, "name", id));
//                    vertices.get(label).add(id);
//                    if(lineCount ++ % 1000 == 0){
//                        // commit the transaction to disk
//                        System.out.println(lineCount + ": Committing transaction ...");
//                        tx.commit();
//                        System.out.println("Transaction committed ...");
//                        tx = graph.newTransaction();
//                    }
                        System.out.println("Creating Label " + label + " with id " + id);
                    } catch(NumberFormatException e){
                        continue;
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                        System.out.println("vertex: Closing buffer reader with number of lines " + lineCount + " read");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

//        System.out.println("Committing transaction ...");
//        tx.commit();
//        System.out.println("Transaction committed ...");
//
//        tx = graph.newTransaction();

        ArrayList<String> vertexList = new ArrayList<>(Arrays.asList(vertexLabels));
        for(String[] edgeFile : edgeFiles){
            if(!vertexList.contains(edgeFile[2])) continue;
            String filename = edgeFile[0] + "_" + edgeFile[1] + "_" + edgeFile[2] + "_0_0.csv";
            BufferedReader br = null;
            String line = "";
            String cvsSplitBy = " ";
            int lineCount = 0;
            try {

                br = new BufferedReader(new FileReader(DATA_PATH + "edge/" + filename));
                System.out.println("Opening buffer reader with on file path " + (DATA_PATH + "edge/" + filename));
                while ((line = br.readLine()) != null) {
                    String[] arr = line.split(cvsSplitBy);
                    if(arr.length<3) continue;
                    try{
                        Vertex head = vertices.get(edgeFile[0]).get(arr[0]);
                        Vertex tail = vertices.get(edgeFile[2]).get(arr[2]);
//                        Vertex head = tx.getVertex(Long.parseLong(arr[0]));
//                        Vertex tail = tx.getVertex(Long.parseLong(arr[1]));
                        head.addEdge(edgeFile[1], tail);
                        System.out.println("Connecting head " + arr[0] + " " + head.toString() +
                            " with label " + edgeFile[0] +
                            " to tail " + arr[2] + " " + tail.toString() +
                            " with label " + edgeFile[2] +
                            " with edge label " + edgeFile[1]);


                        /*if(lineCount ++ % 1000 == 0){
                            // commit the transaction to disk
                            System.out.println(lineCount + ": Committing transaction ...");
                            tx.commit();
                            System.out.println("Transaction committed ...");
                            tx = graph.newTransaction();
                        }*/
                    } catch (NumberFormatException e){
                        continue;
                    }
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                        System.out.println("edge: Closing buffer reader with number of lines " + lineCount + " read");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        // commit the transaction to disk
        System.out.println("Committing transaction ...");
        tx.commit();
        System.out.println("Transaction committed ...");
    }
    public static void main(String args[]) {
        if (null == args || 1 != args.length) {
            System.err.println("Usage: PlayGroundGraphQuery <janusgraph-config-file>");
            System.exit(1);
        }

        JanusGraph g = JanusGraphFactory.open(args[0]);
        load(g);
        g.close();
    }
}
