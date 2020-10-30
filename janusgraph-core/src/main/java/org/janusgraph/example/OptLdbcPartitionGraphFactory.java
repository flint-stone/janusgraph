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
import org.janusgraph.core.TransactionBuilder;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.graphdb.database.StandardJanusGraph;

import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class OptLdbcPartitionGraphFactory {
    static ArrayList<JanusGraph> _graphs = new ArrayList<JanusGraph>();
	static HashMap<String, HashMap<Long, Long>>  _label_ids_map = new HashMap<String, HashMap<Long, Long>>();
    static HashMap<String, String> _name_map = new HashMap<>();
    static ArrayList<String> _cur_files = new ArrayList<String>();
    static String _poperty_file_path = "/home/houbai/codelab/documents/LDBC_properties.txt";
    static String _prefix_path = "";
    static String _folder_name = "";
    static ArrayList<String> vertexLabels = new ArrayList<>();
    static HashSet<String> edgeLabels = new HashSet<>();
    static HashSet<Long> vertices_at_other_partitions = new HashSet<>();
    static HashMap<String, String> propertyTypes = new HashMap<>();
    static HashSet<Long>[] shadow_vertices_at_partitions;
    
    //storage::graph_store* _graph;
    static long _global_id = 1;
    static int _partition_num = 1;
    static int _thread_num = 32;
    static int _tx_batch_size = 40000;
    // static int _tx_total_vertex_cache_size = 2000000;

    static void load_helper() {
        _name_map.put("comment", "Comment");
        _name_map.put("forum", "Forum");
        _name_map.put("organisation", "Organisation");
        _name_map.put("person", "Person");
        _name_map.put("place", "Place");
        _name_map.put("post", "Post");
        _name_map.put("tag", "Tag");
        _name_map.put("tagclass", "TagClass");

        _label_ids_map.put("Comment", new HashMap<Long, Long>());
        _label_ids_map.put("Forum", new HashMap<Long, Long>());
        _label_ids_map.put("Organisation", new HashMap<Long, Long>());
        _label_ids_map.put("Person", new HashMap<Long, Long>());
        _label_ids_map.put("Place", new HashMap<Long, Long>());
        _label_ids_map.put("Post", new HashMap<Long, Long>());
        _label_ids_map.put("Tag", new HashMap<Long, Long>());
        _label_ids_map.put("TagClass", new HashMap<Long, Long>());
    }

    static void get_vertex_files() {
    	_cur_files.clear();
        _cur_files.add(_prefix_path + "comment_0_0.csv");
        _cur_files.add(_prefix_path + "forum_0_0.csv");
        _cur_files.add(_prefix_path + "organisation_0_0.csv");
        _cur_files.add(_prefix_path + "person_0_0.csv");
        _cur_files.add(_prefix_path + "place_0_0.csv");
        _cur_files.add(_prefix_path + "post_0_0.csv");
        _cur_files.add(_prefix_path + "tag_0_0.csv");
        _cur_files.add(_prefix_path + "tagclass_0_0.csv");

        if(vertexLabels.isEmpty()) {
            for(String file_name : _cur_files) {
            	String vertex_label = _name_map.get(file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0]);
            	vertexLabels.add(vertex_label);
            }
        }
    }

    static void get_edge_files() {
    	_cur_files.clear();
        _cur_files.add(_prefix_path + "comment_hasCreator_person_0_0.csv");
        _cur_files.add(_prefix_path + "comment_hasTag_tag_0_0.csv");
        _cur_files.add(_prefix_path + "comment_isLocatedIn_place_0_0.csv");
        _cur_files.add(_prefix_path + "comment_replyOf_comment_0_0.csv");
        _cur_files.add(_prefix_path + "comment_replyOf_post_0_0.csv");

        _cur_files.add(_prefix_path + "forum_containerOf_post_0_0.csv");
        _cur_files.add(_prefix_path + "forum_hasMember_person_0_0.csv");
        _cur_files.add(_prefix_path + "forum_hasModerator_person_0_0.csv");
        _cur_files.add(_prefix_path + "forum_hasTag_tag_0_0.csv");

        _cur_files.add(_prefix_path + "organisation_isLocatedIn_place_0_0.csv");
        _cur_files.add(_prefix_path + "person_hasInterest_tag_0_0.csv");
        _cur_files.add(_prefix_path + "person_isLocatedIn_place_0_0.csv");
        _cur_files.add(_prefix_path + "person_knows_person_0_0.csv");
        _cur_files.add(_prefix_path + "person_likes_comment_0_0.csv");
        _cur_files.add(_prefix_path + "person_likes_post_0_0.csv");
        _cur_files.add(_prefix_path + "person_studyAt_organisation_0_0.csv");
        _cur_files.add(_prefix_path + "person_workAt_organisation_0_0.csv");
        _cur_files.add(_prefix_path + "place_isPartOf_place_0_0.csv");

        _cur_files.add(_prefix_path + "post_hasCreator_person_0_0.csv");
        _cur_files.add(_prefix_path + "post_hasTag_tag_0_0.csv");
        _cur_files.add(_prefix_path + "post_isLocatedIn_place_0_0.csv");
        _cur_files.add(_prefix_path + "tag_hasType_tagclass_0_0.csv");
        _cur_files.add(_prefix_path + "tagclass_isSubclassOf_tagclass_0_0.csv");

        if(edgeLabels.isEmpty()) {
            for(String file_name : _cur_files) {
            	String relation_name = file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0];
            	String[] names = relation_name.split("_");
            	String edge_label = names[1];
            	edgeLabels.add(edge_label);
            }
        }
    }

    static void get_extra_property_files() {
        _cur_files.clear();
        _cur_files.add(_prefix_path + "person_email_emailaddress_0_0.csv");
        _cur_files.add(_prefix_path + "person_speaks_language_0_0.csv");
    }

   static long encode_datetime(String datetime) {
        String[] parts = datetime.split("T");
        long dt_value = 0; 
        String[] date_parts = parts[0].split("-");
        dt_value += Long.valueOf(date_parts[0]); // add year
        dt_value = dt_value * 100 + Long.valueOf(date_parts[1]); // add month
        dt_value = dt_value * 100 + Long.valueOf(date_parts[2]); // add day
        String[] time_parts = parts[1].split("\\+")[0].split("\\.")[0].split(":");
        String mirco_seconds = parts[1].split("\\+")[0].split("\\.")[1];
        dt_value = dt_value * 100 + Long.valueOf(time_parts[0]); // add hour
        dt_value = dt_value * 100 + Long.valueOf(time_parts[1]); // add minute
        dt_value = dt_value * 100 + Long.valueOf(time_parts[2]); // add second
        dt_value = dt_value * 1000 + Long.valueOf(mirco_seconds); // add micro second
        return dt_value;
   }

    static long encode_date(String date) {
        String[] parts = date.split("-");
        long date_value = 0;
        date_value += Long.valueOf(parts[0]); // add year
        date_value = date_value * 100 + Long.valueOf(parts[1]); // add month
        date_value = date_value * 100 + Long.valueOf(parts[2]); // add day
        return date_value;
    }

    static class VtxLoadThread implements Runnable {
        private Thread t = null;
        private int t_id;
        private String vertex_label;
        private int id_loc;
        private String header_line;
        private ArrayList<String> lines = new ArrayList<String>();
        private int[] ptn_counts;

        public VtxLoadThread(int id) {
            this.t_id = id;

            this.ptn_counts = (int[]) new int[_partition_num];
            for(int i = 0; i < _partition_num; i++) {
                this.ptn_counts[i] = 0;
            }
        }

        public void set_vertex_label(String v_label) {
            this.vertex_label = v_label;
        }

        public void set_id_loc(int loc) {
            this.id_loc = loc;
        }

        public void set_header_line(String header) {
            this.header_line = header;
        }

        public void add_line(int ptn_id, String l) {
            this.ptn_counts[ptn_id]++;
            this.lines.add(l);
        }

        public void run() {
            lines.trimToSize();

            JanusGraphTransaction[] txs = new JanusGraphTransaction[(int)_partition_num];
            int[] counts = new int[(int)_partition_num];
            TransactionBuilder tbuilder;

            for(int i = 0; i < _partition_num; i++) {
                counts[i] = 0;

                tbuilder = _graphs.get(i).buildTransaction();
                tbuilder.vertexCacheSize(Math.min(_tx_batch_size, ptn_counts[i]));
                // tbuilder.checkInternalVertexExistence(false);
                // tbuilder.checkExternalVertexExistence(false);
                // tbuilder.consistencyChecks(false);
                txs[i] = tbuilder.start();
            }

            HashMap<Long, Long> id_map = _label_ids_map.get(vertex_label);
            String[] properties = header_line.split("\\|");

            // String line;
            int read_count = 0;
            String[] values;
            long id;
            long g_id;
            int partition_id;
            long janusVertexId;
            Vertex v = null;
            String property;
            for(int idx = 0; idx < lines.size(); idx++) {
                if(++read_count % 20000 == 0) {
                    System.out.println("        [Thread " + t_id + "] processed number : " + read_count);
                }

                values = lines.get(idx).split("\\|");
                id = Long.valueOf(values[id_loc]);
                g_id = id_map.get(id);
                partition_id = (int)(g_id % _partition_num);
                janusVertexId = ((StandardJanusGraph) _graphs.get(partition_id)).getIDManager().toVertexId(g_id);
                
                v = txs[partition_id].addVertex(T.id, janusVertexId, T.label, vertex_label);
                for(int i = 0; i < properties.length; i++) {
                    property = properties[i];
                    if(propertyTypes.get(property).equals("String")) {
                        v.property(property, values[i]);
                    }
                    else {
                        if(properties[i].equals("creationDate") || properties[i].equals("joinDate")) {
                            v.property(property, encode_datetime(values[i]));
                        } 
                        else if(properties[i].equals("birthday")) {
                            v.property(property, encode_date(values[i]));
                        }
                        else {
                            v.property(property, Long.valueOf(values[i]));
                        }
                    }
                }

                if(++counts[partition_id] >= _tx_batch_size) {
                    txs[partition_id].commit();
                    txs[partition_id] = null;
                    counts[partition_id] = 0;

                    tbuilder = _graphs.get(partition_id).buildTransaction();
                    tbuilder.vertexCacheSize(_tx_batch_size);
                    // tbuilder.checkInternalVertexExistence(false);
                    // tbuilder.checkExternalVertexExistence(false);
                    // tbuilder.consistencyChecks(false);
                    txs[partition_id] = tbuilder.start();
                }

                v = null;
            }

            for(int i = 0; i < _partition_num; i++) {
                if(counts[i] != 0) {
                    txs[i].commit();
                    txs[i] = null;
                }
            }

            txs = null;
            tbuilder = null;
            id_map = null;
            clear();
        }

        public void start() {
            if (t == null) {
                t = new Thread(this, String.valueOf(t_id));
                t.start();
            }
        }

        public void join() {
            try {
                t.join();
            }
            catch(Exception e) {
                System.out.println("error in load_vertex: " + e.getMessage());
            }
        }

        public void clear() {
            lines.clear();
            lines.trimToSize();
            lines = null;
        }

        public void finalize() {
            lines = null;
            t = null;
            ptn_counts = null;
        }
    }

   static void load_vertex() {
       get_vertex_files();
       VtxLoadThread[] loaders = new VtxLoadThread[_thread_num];

       String vertex_label;
       HashMap<Long, Long> id_map;
       String line;
       String[] properties;
       int id_loc;
       int line_readed;
       String[] values;
       long id;
       long g_id;
       for(String file_name : _cur_files) {
            // Create thread loaders
            for(int i = 0; i < _thread_num; i++) {
                loaders[i] = new VtxLoadThread(i);
            }

            // Parse vertex label
            vertex_label = _name_map.get(file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0]);
            for(int i = 0; i < _thread_num; i++) {
                loaders[i].set_vertex_label(vertex_label);
            }
            System.out.println("Loading: "+ vertex_label + " id from " + _global_id);
            if(!_label_ids_map.containsKey(vertex_label))
                _label_ids_map.put(vertex_label, new HashMap<>());
            id_map = _label_ids_map.get(vertex_label);
            
            // Read files
            try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
                // Read header line
                line = br.readLine();
                properties = line.split("\\|");
                id_loc = properties.length + 1;
                for(int i = 0; i < properties.length; i++) {
                    if(properties[i].equals("id")) {
                        id_loc = i;
                        break;
                    }
                }
                if(id_loc > properties.length) {
                    System.out.println("Error in vertex proporties!");
                    return;
                }
                // Set header line
                for(int i = 0; i < _thread_num; i++) {
                    loaders[i].set_header_line(line);
                    loaders[i].set_id_loc(id_loc);
                }

                // Store lines in file
                line_readed = 0;
                while ((line = br.readLine()) != null) {
                    values = line.split("\\|");
                    id = Long.valueOf(values[id_loc]);
                    g_id = _global_id++;
                    id_map.put(id, g_id);

                    loaders[line_readed++ % _thread_num].add_line((int)(g_id % _partition_num), line);
                    if(line_readed % 1000000 == 0)
                        System.out.println("        CSV records read number : " + line_readed);
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in load_vertex: " + e.getMessage());
            }

            for(int i = 0; i < _thread_num; i++) {
                loaders[i].start();
            }
            for(int i = 0; i < _thread_num; i++) {
                loaders[i].join();
            }
            for(int i = 0; i < _thread_num; i++) {
                loaders[i] = null;
            }

            System.gc();
            System.runFinalization();

            System.out.println("Loaded: " + vertex_label + " id to " + _global_id);
        }
    }

    static class shadow_insert_unit {
        public int p_id;
        public long shadow_v_id;
        public String shadow_v_label;

        public shadow_insert_unit(int partition, long shadow_vertex_id, String shadow_vertex_label) {
            this.p_id = partition;
            this.shadow_v_id = shadow_vertex_id;
            this.shadow_v_label = shadow_vertex_label;
        }
    }

    static class ShadowVtxLoadThread implements Runnable {
        private Thread t = null;
        private int t_id;
        private ArrayList<shadow_insert_unit> lines = new ArrayList<shadow_insert_unit>();
        private int[] ptn_counts;

        public ShadowVtxLoadThread(int id) {
            this.t_id = id;

            this.ptn_counts = (int[]) new int[_partition_num];
            for(int i = 0; i < _partition_num; i++) {
                this.ptn_counts[i] = 0;
            }
        }

        public void add_insert_unit(int partition, long shadow_vertex_id, String shadow_vertex_label) {
            this.ptn_counts[partition]++;
            shadow_insert_unit new_line = new shadow_insert_unit(partition, shadow_vertex_id, shadow_vertex_label);
            this.lines.add(new_line);
            new_line = null;
        }

        public void run() {
            lines.trimToSize();
            JanusGraphTransaction[] txs = new JanusGraphTransaction[(int)_partition_num];
            int[] counts = new int[(int)_partition_num];
            TransactionBuilder tbuilder;

            for(int i = 0; i < _partition_num; i++) {
                counts[i] = 0;

                tbuilder = _graphs.get(i).buildTransaction();
                tbuilder.vertexCacheSize(Math.min(_tx_batch_size, ptn_counts[i]));
                // tbuilder.checkInternalVertexExistence(false);
                // tbuilder.checkExternalVertexExistence(false);
                // tbuilder.consistencyChecks(false);
                txs[i] = tbuilder.start();
            }

            int read_count = 0;
            int partition_id;
            shadow_insert_unit insert_unit;
            for(int idx = 0; idx < lines.size(); idx++) {
                if(++read_count % 20000 == 0)
                    System.out.println("        [Thread " + t_id + "] Loading shadow vertices, processed number : " + read_count);
                insert_unit = lines.get(idx);
                partition_id = insert_unit.p_id;
                txs[partition_id].addVertex(T.id, insert_unit.shadow_v_id, T.label, insert_unit.shadow_v_label);

                if(++counts[partition_id] >= _tx_batch_size) {
                    txs[partition_id].commit();
                    txs[partition_id] = null;
                    counts[partition_id] = 0;

                    tbuilder = _graphs.get(partition_id).buildTransaction();
                    tbuilder.vertexCacheSize(_tx_batch_size);
                    // tbuilder.checkInternalVertexExistence(false);
                    // tbuilder.checkExternalVertexExistence(false);
                    // tbuilder.consistencyChecks(false);
                    txs[partition_id] = tbuilder.start();
                }
            }

            for(int i = 0; i < _partition_num; i++) {
                if(counts[i] != 0) {
                    txs[i].commit();
                    txs[i] = null;
                }
            }

            txs = null;
            counts = null;
            tbuilder = null;
            clear();
        }

        public void start() {
            if (t == null) {
                t = new Thread(this, String.valueOf(t_id));
                t.start();
            }
        }

        public void join() {
            try {
                t.join();
            }
            catch(Exception e) {
                System.out.println("error in load_shadow_vertex: " + e.getMessage());
            }
        }

        public void clear() {
            lines.clear();
            lines.trimToSize();
            lines = null;
        }

        public void finalize() {
            lines = null;
            t = null;
            ptn_counts = null;
        }
    }

    static class EdgeLoadThread implements Runnable {
        private Thread t = null;
        private int t_id;
        private String source_label;
        private String edge_label;
        private String dest_label;
        private int src_id_loc;
        private int dst_id_loc;
        private String header_line;
        private ArrayList<String> lines = new ArrayList<String>();

        public EdgeLoadThread(int id) {
            this.t_id = id;
        }

        public void set_labels(String s_label, String e_label, String d_label) {
            this.source_label = s_label;
            this.edge_label = e_label;
            this.dest_label = d_label;
        }

        public void set_id_locs(int s_loc, int d_loc) {
            this.src_id_loc = s_loc;
            this.dst_id_loc = d_loc;
        }

        public void set_header_line(String header) {
            this.header_line = header;
        }

        public void add_line(String l) {
            this.lines.add(l);
        }

        public void run() {
            lines.trimToSize();
            JanusGraphTransaction[] txs = new JanusGraphTransaction[(int)_partition_num];
            int[] counts = new int[(int)_partition_num];
            TransactionBuilder tbuilder;

            for(int i = 0; i < _partition_num; i++) {
                counts[i] = 0;

                tbuilder = _graphs.get(i).buildTransaction();
                tbuilder.vertexCacheSize(_tx_batch_size);
                // tbuilder.checkInternalVertexExistence(false);
                // tbuilder.checkExternalVertexExistence(false);
                // tbuilder.consistencyChecks(false);
                txs[i] = tbuilder.start();
            }

            String[] properties = header_line.split("\\|");
            int read_count = 0;
            String[] values;
            long local_src;
            long local_dest;
            long global_src;
            long global_dest;
            int src_partition_id;
            int dest_partition_id;
            long janus_src_id;
            long janus_dest_id;
            Vertex src = null;
            Vertex dest = null;
            long janus_shadow_src_id_in_dest_partition;
            long janus_shadow_dest_id_in_src_partition;
            Vertex shadow_dest = null;
            Vertex shadow_src = null;
            Edge e1 = null;
            Edge e2 = null;
            for(int idx = 0; idx < lines.size(); idx++) {
                if(++read_count % 20000 == 0)
                    System.out.println("        [Thread " + t_id + "] Loading edges, processed number : " + read_count);

                values = lines.get(idx).split("\\|");

                local_src = Long.valueOf(values[src_id_loc]);
                local_dest = Long.valueOf(values[dst_id_loc]);
                global_src = _label_ids_map.get(source_label).get(local_src);
                global_dest = _label_ids_map.get(dest_label).get(local_dest);
                src_partition_id = (int) (global_src % _partition_num);
                dest_partition_id = (int) (global_dest % _partition_num);
                janus_src_id = ((StandardJanusGraph) _graphs.get(src_partition_id)).getIDManager().toVertexId(global_src);
                janus_dest_id = ((StandardJanusGraph) _graphs.get(dest_partition_id)).getIDManager().toVertexId(global_dest);
                src = txs[src_partition_id].getVertex(janus_src_id);
                dest = txs[dest_partition_id].getVertex(janus_dest_id);

                if(src_partition_id == dest_partition_id) {
                    e1 = src.addEdge(edge_label, dest);
                    counts[src_partition_id]++;
                } else {
                    janus_shadow_src_id_in_dest_partition = ((StandardJanusGraph) _graphs.get(dest_partition_id)).getIDManager().toVertexId(global_src);
                    janus_shadow_dest_id_in_src_partition = ((StandardJanusGraph) _graphs.get(src_partition_id)).getIDManager().toVertexId(global_dest);
                    shadow_dest = txs[src_partition_id].getVertex(janus_shadow_dest_id_in_src_partition);
                    shadow_src = txs[dest_partition_id].getVertex(janus_shadow_src_id_in_dest_partition);
                    e1 = src.addEdge(edge_label, shadow_dest);
                    e2 = shadow_src.addEdge(edge_label, dest);
                    counts[src_partition_id]++;
                    counts[dest_partition_id]++;
                }

                for(int i=0; i<properties.length; i++) {
                    if(i == src_id_loc || i == dst_id_loc) continue;
                    String property = properties[i];
                    if(propertyTypes.get(property).equals("String")) {
                        if(e1 != null) {
                            e1.property(property, values[i]);
                        }
                        if(e2 != null) {
                            e2.property(property, values[i]);
                        }
                    }
                    else {
                        if(property.equals("creationDate") || property.equals("joinDate")) {
                            if(e1 != null) {
                                e1.property(property, encode_datetime(values[i]));
                            }
                            if(e2 != null) {
                                e2.property(property, encode_datetime(values[i]));
                            }
                        } 
                        else {
                            if(e1 != null) {
                                e1.property(property, Long.valueOf(values[i]));
                            }
                            if(e2 != null) {
                                e2.property(property, Long.valueOf(values[i]));
                            }
                        }
                    }
                }

                if(counts[src_partition_id] >= _tx_batch_size) {
                    txs[src_partition_id].commit();
                    txs[src_partition_id] = null;
                    counts[src_partition_id] = 0;

                    tbuilder = _graphs.get(src_partition_id).buildTransaction();
                    tbuilder.vertexCacheSize(_tx_batch_size);
                    // tbuilder.checkInternalVertexExistence(false);
                    // tbuilder.checkExternalVertexExistence(false);
                    // tbuilder.consistencyChecks(false);
                    txs[src_partition_id] = tbuilder.start();
                }
                if(counts[dest_partition_id] >= _tx_batch_size) {
                    txs[dest_partition_id].commit();
                    txs[dest_partition_id] = null;
                    counts[dest_partition_id] = 0;

                    tbuilder = _graphs.get(dest_partition_id).buildTransaction();
                    tbuilder.vertexCacheSize(_tx_batch_size);
                    // tbuilder.checkInternalVertexExistence(false);
                    // tbuilder.checkExternalVertexExistence(false);
                    // tbuilder.consistencyChecks(false);
                    txs[dest_partition_id] = tbuilder.start();
                }

                src = null;
                dest = null;
                shadow_dest = null;
                shadow_src = null;
                e1 = null;
                e2 = null;
            }


            for(int i = 0; i < _partition_num; i++) {
                if(counts[i] != 0) {
                    txs[i].commit();
                    txs[i] = null;
                }
            }

            txs = null;
            counts = null;
            tbuilder = null;
            clear();
        }

        public void start() {
            if (t == null) {
                t = new Thread(this, String.valueOf(t_id));
                t.start();
            }
        }

        public void join() {
            try {
                t.join();
            }
            catch(Exception e) {
                System.out.println("error in load_edge: " + e.getMessage());
            }
        }

        public void clear() {
            lines.clear();
            lines.trimToSize();
            lines = null;
        }

        public void finalize() {
            lines = null;
            t = null;
        }
    }

    static void load_edges() {
        get_edge_files();
        EdgeLoadThread[] loaders = new EdgeLoadThread[_thread_num];
        ShadowVtxLoadThread[] shadow_loaders = new ShadowVtxLoadThread[_thread_num];
        // JanusGraphTransaction[] txs = new JanusGraphTransaction[(int)_partition_num];

        // Init shadow vertex sets
        shadow_vertices_at_partitions = (HashSet<Long>[]) new HashSet[(int)_partition_num];
        for(int i = 0; i < _partition_num; i++) {
            shadow_vertices_at_partitions[i] = new HashSet<Long>();
        }

        String relation_name;
        String[] names;
        String source_label;
        String edge_label;
        String dest_label;
        String line;
        String[] properties;
        int length;
        int src_id_loc;
        int dst_id_loc;
        int line_readed;
        int shadow_v_num;
        String[] values;
        long local_src;
        long local_dest;
        long global_src;
        long global_dest;
        int src_partition_id;
        int dest_partition_id;
        long janus_shadow_src_id_in_dest_partition;
        long janus_shadow_dest_id_in_src_partition;

        for(String file_name : _cur_files) {
            // for(int i = 0; i < _partition_num; i++) {
            //     txs[i] = _graphs.get(i).newTransaction();
            // }

            // Create thread loaders
            for(int i = 0; i < _thread_num; i++) {
                loaders[i] = new EdgeLoadThread(i);
                shadow_loaders[i] = new ShadowVtxLoadThread(i);
            }

            // Parse labels
            relation_name = file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0];
            names = relation_name.split("_");
            source_label = _name_map.get(names[0]);
            edge_label = names[1];
            dest_label = _name_map.get(names[2]);
            for(int i = 0; i < _thread_num; i++) {
                loaders[i].set_labels(source_label, edge_label, dest_label);
            }
            System.out.println("Loading "+ source_label+" "+edge_label+" "+dest_label);

            // Read files
            try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
                // Read header line
                line = br.readLine();
                properties = line.split("\\|");
                length = properties.length;
                src_id_loc = length + 1;
                dst_id_loc = length + 1;
                for(int i=0; i<length; i++) {
                    if(properties[i].equals(source_label + ".id")) {
                        src_id_loc = i;
                        break;
                    }
                }
                for(int i=src_id_loc+1; i<length; i++) {
                    if(properties[i].equals(dest_label + ".id")) {
                        dst_id_loc = i;
                        break;
                    }
                }
                if(src_id_loc > length || dst_id_loc > length) {
                    System.out.println("Error in vertex proporties!");
                    return;
                }
                // Set header line
                for(int i = 0; i < _thread_num; i++) {
                    loaders[i].set_header_line(line);
                    loaders[i].set_id_locs(src_id_loc, dst_id_loc);
                }

                // Store lines in file
                line_readed = 0;
                shadow_v_num = 0;
                while ((line = br.readLine()) != null) {
                    loaders[line_readed++ % _thread_num].add_line(line);

                    values = line.split("\\|");
                    local_src = Long.valueOf(values[src_id_loc]);
                    local_dest = Long.valueOf(values[dst_id_loc]);
                    global_src = _label_ids_map.get(source_label).get(local_src);
                    global_dest = _label_ids_map.get(dest_label).get(local_dest);
                    src_partition_id = (int) (global_src % _partition_num);
                    dest_partition_id = (int) (global_dest % _partition_num);

                    if(src_partition_id != dest_partition_id) {
                        janus_shadow_src_id_in_dest_partition = ((StandardJanusGraph) _graphs.get(dest_partition_id)).getIDManager().toVertexId(global_src);
                        janus_shadow_dest_id_in_src_partition = ((StandardJanusGraph) _graphs.get(src_partition_id)).getIDManager().toVertexId(global_dest);

                        if(!shadow_vertices_at_partitions[src_partition_id].contains(global_dest)) {
                            shadow_loaders[shadow_v_num++ % _thread_num].add_insert_unit(src_partition_id, janus_shadow_dest_id_in_src_partition, dest_label);
                            shadow_vertices_at_partitions[src_partition_id].add(global_dest);
                        }
                        if(!shadow_vertices_at_partitions[dest_partition_id].contains(global_src)) {
                            shadow_loaders[shadow_v_num++ % _thread_num].add_insert_unit(dest_partition_id, janus_shadow_src_id_in_dest_partition, source_label);
                            shadow_vertices_at_partitions[dest_partition_id].add(global_src);
                        }
                    }
                    
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in load_edges: "+e.getMessage());
            }

            // for(int i = 0; i < _partition_num; i++) {
            //     txs[i].commit();
            // }

            for(int i = 0; i < _thread_num; i++) {
                shadow_loaders[i].start();
            }
            for(int i = 0; i < _thread_num; i++) {
                shadow_loaders[i].join();
            }
            for(int i = 0; i < _thread_num; i++) {
                shadow_loaders[i] = null;
            }
            System.gc();
            System.runFinalization();

            for(int i = 0; i < _thread_num; i++) {
                loaders[i].start();
            }
            for(int i = 0; i < _thread_num; i++) {
                loaders[i].join();
            }
            for(int i = 0; i < _thread_num; i++) {
                loaders[i] = null;
            }

            System.gc();
            System.runFinalization();
        }

        for(int i = 0; i < _partition_num; i++) {
            shadow_vertices_at_partitions[i].clear();
            shadow_vertices_at_partitions[i] = null;
        }
        shadow_vertices_at_partitions = null;
    }

    static void load_extra_properties() {
        get_extra_property_files();
        int[] counts = new int[(int)_partition_num];
        JanusGraphTransaction[] txs = new JanusGraphTransaction[(int)_partition_num];
        TransactionBuilder tbuilder;
        for(String file_name : _cur_files) {
            for(int i = 0; i < _partition_num; i++) {
                counts[i] = 0;

                tbuilder = _graphs.get(i).buildTransaction();
                tbuilder.vertexCacheSize(_tx_batch_size);
                // tbuilder.checkInternalVertexExistence(false);
                // tbuilder.checkExternalVertexExistence(false);
                // tbuilder.consistencyChecks(false);
                txs[i] = tbuilder.start();
            }

            String prop_relation_name = file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0];
            String[] names = prop_relation_name.split("_");
            String vertex_label = _name_map.get(names[0]);
            String vertex_property = names[1];
            String vertex_property_entity = names[2];
            System.out.println("Loading extra vertex properties "+ vertex_label+" "+vertex_property+" "+vertex_property_entity);

            try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
                String line;
                line = br.readLine();

                // Initialize edge properties
                String[] properties = line.split("\\|");
                int length = properties.length;
                
                long last_vertex = -1;
                String props = "";
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("\\|");
                    long vertex = Long.valueOf(values[1]);
                    if(vertex != last_vertex) {
                        if(last_vertex >= 0) {
                            long dump_vertex = _label_ids_map.get(vertex_label).get(last_vertex);
                            int partition_id = (int)(dump_vertex % _partition_num);
                            Vertex v = txs[partition_id].getVertex(((StandardJanusGraph) _graphs.get(partition_id)).getIDManager().toVertexId(dump_vertex));
                            v.property(vertex_property, props.substring(0, props.length()-1));
                            props = "";
                            if(counts[partition_id]++ >= _tx_batch_size) {
                                txs[partition_id].commit();
                                counts[partition_id] = 0;

                                tbuilder = _graphs.get(partition_id).buildTransaction();
                                tbuilder.vertexCacheSize(_tx_batch_size);
                                // tbuilder.checkInternalVertexExistence(false);
                                // tbuilder.checkExternalVertexExistence(false);
                                // tbuilder.consistencyChecks(false);
                                txs[partition_id] = tbuilder.start();
                            }
                        }
                        last_vertex = vertex;
                    }
                    props += (values[2] + " ");
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in load_extra_properties: "+e.getMessage());
            }
            for(int i = 0; i < _partition_num; i++) {
                if(counts[i] != 0) {
                    txs[i].commit();
                }
            }
        }
    }

    static void dump_label_map() {
        String[] all_labels = { 
            "Comment",
            "Forum",
            "Organisation",
            "Person",
            "Place",
            "Post",
            "Tag",
            "TagClass",
        };

        for(int i = 0; i < 8; i++) {
            String dump_file_path = "/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/db/bdbje-partitions/" + _folder_name + "/label_map/" + all_labels[i];

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(dump_file_path))) {
                Iterator<HashMap.Entry<Long, Long>> entries = _label_ids_map.get(all_labels[i]).entrySet().iterator();
                while (entries.hasNext()) {
                    HashMap.Entry<Long, Long> entry = entries.next();
                    bw.write(entry.getKey() + " " + entry.getValue());
                    if(entries.hasNext()) {
                        bw.newLine();
                    }
                }
                bw.flush();
                bw.close();
            }
            catch(Exception e) {
                System.out.println("error in dump_label_map: "+e.getMessage());
            }
        }
    }

    static void load_label_map() {
        String[] all_labels = { 
            "Comment",
            "Forum",
            "Organisation",
            "Person",
            "Place",
            "Post",
            "Tag",
            "TagClass",
        };

        for(int i = 0; i < 8; i++) {
            String load_file_path = "/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/db/bdbje-partitions/" + _folder_name + "/label_map/" + all_labels[i];
            
            HashMap<Long, Long> id_map = _label_ids_map.get(all_labels[i]);
            id_map.clear();
            try (BufferedReader br = new BufferedReader(new FileReader(load_file_path))) {
                String line;
                while((line = br.readLine()) != null) {
                    String[] values = line.split(" ");
                    id_map.put(Long.valueOf(values[0]), Long.valueOf(values[1]));
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in load_label_map: "+e.getMessage());
            }
        }
    }

    static void make_label_schema() {
        ArrayList<String> _vertex_labels = new ArrayList<String>();
        _vertex_labels.add("Comment");
        _vertex_labels.add("Organisation");
        _vertex_labels.add("Post");
        _vertex_labels.add("Tag");
        _vertex_labels.add("TagClass");
        _vertex_labels.add("Person");
        _vertex_labels.add("Place");
        _vertex_labels.add("Forum");

        ArrayList<String> _edge_labels = new ArrayList<String>();
        _edge_labels.add("hasCreator");
        _edge_labels.add("studyAt");
        _edge_labels.add("hasTag");
        _edge_labels.add("workAt");
        _edge_labels.add("hasMember");
        _edge_labels.add("isPartOf");
        _edge_labels.add("hasModerator");
        _edge_labels.add("hasInterest");
        _edge_labels.add("isLocatedIn");
        _edge_labels.add("containerOf");
        _edge_labels.add("isSubclassOf");
        _edge_labels.add("replyOf");
        _edge_labels.add("hasType");
        _edge_labels.add("knows");
        _edge_labels.add("likes");

        for(JanusGraph g: _graphs) {
            JanusGraphManagement management = g.openManagement();
            for(String v_label: _vertex_labels) {
                management.makeVertexLabel(v_label).make();
            }
            for(String e_label: _edge_labels) {
                management.makeEdgeLabel(e_label).make();
            }
            management.commit();
        }
    }
    
    static void load_property_types() {
        try (BufferedReader br = new BufferedReader(new FileReader(_poperty_file_path))) {
            String line;
            line = br.readLine();
            while(line != null) {
                String name = line.split(" ")[0];
                String type = line.split(" ")[1];
                propertyTypes.put(name, type);
                line = br.readLine();
            }
            br.close();
        }
        catch(Exception e) {
            System.out.println("error: "+e.getMessage());
        }
    }

    static void make_property_schema() {
        for(JanusGraph g: _graphs) {
            JanusGraphManagement management = g.openManagement();
            Iterator<HashMap.Entry<String, String>> entries = propertyTypes.entrySet().iterator();
            while (entries.hasNext()) {
                HashMap.Entry<String, String> entry = entries.next();
                String name = entry.getKey();
                String type = entry.getValue();
                if(type.equals("String")) {
                    management.makePropertyKey(name).dataType(String.class).make();
                }
                else {
                    management.makePropertyKey(name).dataType(Long.class).make();
                }
            }
            management.commit();
        }
    }

    public static JanusGraph create(final String directory) {
        JanusGraphFactory.Builder config = JanusGraphFactory.build();
        config.set("storage.backend", "berkeleyje");
        config.set("storage.directory", directory);
        // config.set("index." + INDEX_NAME + ".backend", "elasticsearch");

        JanusGraph graph = config.open();
        ExpGraphFactory.load(graph);
        return graph;
    }

    private static boolean mixedIndexNullOrExists(StandardJanusGraph graph, String indexName) {
        return indexName == null || graph.getIndexSerializer().containsIndex(indexName);
    }

    public static void dedupe_vertex_schema_files(int p_id) {

        String[] in_files = { 
            Integer.toString(p_id) + "_typeIDToPropertyName.csv",
            // Integer.toString(p_id) + "_typeIDToPropertyNameRemaining.csv",
            Integer.toString(p_id) + "_typeIDToVertexLabel.csv",
            // Integer.toString(p_id) + "_typeIDToEdgeLabel.csv",
        };
        String[] out_files = {
            "db/bdbje-partitions/typeIDToPropertyName.csv",
            "db/bdbje-partitions/typeIDToVertexLabel.csv",
        };
        HashMap<String, Integer> properties_to_id = new HashMap<>();
        HashMap<String, Integer> properties_to_type = new HashMap<>();
        HashMap<String, Integer> labels = new HashMap<>();
        // HashSet<String> contents = new HashSet<>();


        for(int i=0; i<1; i++) {
            properties_to_id.clear();
            properties_to_type.clear();
            
            try (BufferedReader br = new BufferedReader(new FileReader(in_files[i]))) {
                String line = br.readLine();
                while(line != null) {
                    String[] values = line.split(",");
                    if(propertyTypes.containsKey(values[1])) {
                        properties_to_id.put(values[1], Integer.valueOf(values[0]));
                        properties_to_type.put(values[1], Integer.valueOf(values[2]));
                    }
                    line = br.readLine();
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(PropertyName Read): "+e.getMessage());
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(out_files[i]))) {
                Iterator<HashMap.Entry<String, Integer>> entries = properties_to_id.entrySet().iterator();
                while (entries.hasNext()) {
                    HashMap.Entry<String, Integer> entry = entries.next();
                    bw.write(entry.getValue() + "," + entry.getKey() + "," + properties_to_type.get(entry.getKey()));
                    if(entries.hasNext()) {
                        bw.newLine();
                    }
                }
                bw.flush();
                bw.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(PropertyName Write): "+e.getMessage());
            }
        }

        for(int i=1; i<2; i++) {
            labels.clear();

            try (BufferedReader br = new BufferedReader(new FileReader(in_files[i]))) {
                String line = br.readLine();
                while(line != null) {
                    String[] values = line.split(",");
                    if(vertexLabels.contains(values[1]) || edgeLabels.contains(values[1])) {
                        labels.put(values[1], Integer.valueOf(values[0]));
                        // System.out.println("label: "+values[1]+", id: "+values[0]);
                    }
                    if(i == 2 && labels.size() == vertexLabels.size()) break;
                    line = br.readLine();
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(Vertex/Edge label read): "+e.getMessage());
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(out_files[i]))) {
                Iterator<HashMap.Entry<String, Integer>> entries = labels.entrySet().iterator();
                while (entries.hasNext()) {
                    HashMap.Entry<String, Integer> entry = entries.next();
                    bw.write(entry.getValue() + "," + entry.getKey());
                    if(entries.hasNext()) {
                        bw.newLine();
                    }
                }
                bw.flush();
                bw.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(Vertex/Edge label write): "+e.getMessage());
            }
        }
    }

    public static void dedupe_edge_schema_files(int p_id) {

        String[] in_files = {
            Integer.toString(p_id) + "_typeIDToPropertyNameRemaining.csv",
            Integer.toString(p_id) + "_typeIDToEdgeLabel.csv",
        };
        String[] out_files = {
            "db/bdbje-partitions/typeIDToPropertyNameRemaining.csv",
            "db/bdbje-partitions/typeIDToEdgeLabel.csv",
        };
        HashMap<String, Integer> properties_to_id = new HashMap<>();
        HashMap<String, Integer> properties_to_type = new HashMap<>();
        HashMap<String, Integer> labels = new HashMap<>();
        // HashSet<String> contents = new HashSet<>();


        for(int i=0; i<1; i++) {
            properties_to_id.clear();
            properties_to_type.clear();
            
            try (BufferedReader br = new BufferedReader(new FileReader(in_files[i]))) {
                String line = br.readLine();
                while(line != null) {
                    String[] values = line.split(",");
                    if(propertyTypes.containsKey(values[1])) {
                        properties_to_id.put(values[1], Integer.valueOf(values[0]));
                        properties_to_type.put(values[1], Integer.valueOf(values[2]));
                    }
                    line = br.readLine();
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(PropertyName Read): "+e.getMessage());
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(out_files[i]))) {
                Iterator<HashMap.Entry<String, Integer>> entries = properties_to_id.entrySet().iterator();
                while (entries.hasNext()) {
                    HashMap.Entry<String, Integer> entry = entries.next();
                    bw.write(entry.getValue() + "," + entry.getKey() + "," + properties_to_type.get(entry.getKey()));
                    if(entries.hasNext()) {
                        bw.newLine();
                    }
                }
                bw.flush();
                bw.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(PropertyName Write): "+e.getMessage());
            }
        }

        for(int i=1; i<2; i++) {
            labels.clear();

            try (BufferedReader br = new BufferedReader(new FileReader(in_files[i]))) {
                String line = br.readLine();
                while(line != null) {
                    String[] values = line.split(",");
                    if(vertexLabels.contains(values[1]) || edgeLabels.contains(values[1])) {
                        labels.put(values[1], Integer.valueOf(values[0]));
                        // System.out.println("label: "+values[1]+", id: "+values[0]);
                    }
                    if(i == 2 && labels.size() == vertexLabels.size()) break;
                    line = br.readLine();
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(Vertex/Edge label read): "+e.getMessage());
            }

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(out_files[i]))) {
                Iterator<HashMap.Entry<String, Integer>> entries = labels.entrySet().iterator();
                while (entries.hasNext()) {
                    HashMap.Entry<String, Integer> entry = entries.next();
                    bw.write(entry.getValue() + "," + entry.getKey());
                    if(entries.hasNext()) {
                        bw.newLine();
                    }
                }
                bw.flush();
                bw.close();
            }
            catch(Exception e) {
                System.out.println("error in dedupe_schema_files(Vertex/Edge label write): "+e.getMessage());
            }
        }
    }

    public static void load(final JanusGraph graph) {
        // //  _partition_num = partition_number;
        // //  _cur_partition = current_partition;

        //  //Create Schema
        //  load_helper();
        //  load_property(graph);

        //  // Load graph
        //  load_vertex(graph);
        //  load_edges(graph);
        //  load_extra_properties(graph);
        //  dedupe_schema_files();
    }

    public static void load_graph_vertices(String folder_name, int partition_number) {
        // Init graphs
        _partition_num = partition_number;
        _folder_name = folder_name;
        _prefix_path = "/home/houbai/codelab/documents/data/" + _folder_name + "/";
        load_helper();
        get_vertex_files();
        get_edge_files();

        for(int i = 0; i < _partition_num; i++) {
            _graphs.add(i, JanusGraphFactory.open("conf/berkeleyje-partition/janusgraph-berkeleyje-p" + i + ".properties"));
        }
        make_label_schema();
        load_property_types();
        make_property_schema();

        // Load vertex
        load_vertex();
        load_extra_properties();
        dump_label_map();

        for(int i = 0; i < _partition_num; i++) {
            _graphs.get(i).close();
        }
        // dedupe_vertex_schema_files(0);
    }

    public static void load_graph_edges(String folder_name, int partition_number) {
        // Init graphs
        _partition_num = partition_number;
        _folder_name = folder_name;
        _prefix_path = "/home/houbai/codelab/documents/data/" + _folder_name + "/";
        load_helper();
        get_vertex_files();
        get_edge_files();

        for(int i = 0; i < _partition_num; i++) {
            _graphs.add(i, JanusGraphFactory.open("conf/berkeleyje-partition/janusgraph-berkeleyje-p" + i + ".properties"));
        }
        load_property_types();

        // Load edges
        load_label_map();
        load_edges();

        for(int i = 0; i < _partition_num; i++) {
            _graphs.get(i).close();
        }
        // dedupe_edge_schema_files(0);
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
         System.err.println("Usage: ExpGraphFactory <janusgraph-config-file>");
         System.exit(1);
     }

     JanusGraph g = JanusGraphFactory.open(args[0]);
     load(g);
     g.close();
 }
}


