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

import java.io.*;
import java.util.*;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class LdbcGraphFactory {
	static HashMap<String, HashMap<Long, Long>>  _label_ids_map = new HashMap<String, HashMap<Long, Long>>();
    static HashMap<String, String> _name_map = new HashMap<>();
    static ArrayList<String> _cur_files = new ArrayList<String>();
    static String _poperty_file_path = "/home/houbai/documents/LDBC_properties.txt";
    static String _prefix_path = "/home/houbai/documents/data/ldbc_graph_0.1/";
    static String _folder_name = "ldbc_graph_0.1";
    static ArrayList<String> vertexLabels = new ArrayList<>();
    static HashSet<String> edgeLabels = new HashSet<>();
    static HashMap<String, String>propertyTypes = new HashMap<>();
    
    //storage::graph_store* _graph;
    static long _global_id = 1;

    static long _partition_num = 1;
    static long _cur_partition = 0;

    class edge_id {
        long src_id;
        long inner_id;
        long dst_id;

		public edge_id(long src_id, long inner_id, long dst_id) {
			this.src_id = src_id;
			this.inner_id = inner_id;
			this.dst_id = dst_id;
		}

    }
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
        //_cur_files.add(_prefix_path + "person_email_emailaddress_0_0.csv");
        _cur_files.add(_prefix_path + "person_hasInterest_tag_0_0.csv");
        _cur_files.add(_prefix_path + "person_isLocatedIn_place_0_0.csv");
        _cur_files.add(_prefix_path + "person_knows_person_0_0.csv");
        _cur_files.add(_prefix_path + "person_likes_comment_0_0.csv");
        _cur_files.add(_prefix_path + "person_likes_post_0_0.csv");
        //_cur_files.add(_prefix_path + "person_speaks_language_0_0.csv");
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
        dt_value = dt_value * 100 + Long.valueOf(mirco_seconds); // add micro second
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

   static void load_vertex(final JanusGraph graph) {
       get_vertex_files();
        for(String file_name : _cur_files) {
            int loaded_num = 0;
     	    int count = 0;
     	    JanusGraphTransaction tx = graph.newTransaction();
        	String vertex_label = _name_map.get(file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0]);
            System.out.print("Loading: "+ vertex_label + " id from " + _global_id);
        	HashMap<Long, Long> id_map;
        	if(!_label_ids_map.containsKey(vertex_label))
        		_label_ids_map.put(vertex_label, new HashMap<>());
        	id_map = _label_ids_map.get(vertex_label);
            try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
                String line;
                line = br.readLine();
                
                // Initialize vertex properties
                String[] properties = line.split("\\|");
                int length = properties.length;
                int id_loc = length + 1;
                for(int i=0; i<length; i++) {
                    if(properties[i].equals("id")) {
                        id_loc = i;
                        break;
                    }
                }
                if(id_loc > length) {
                    System.out.println("Error in vertex proporties!");
                    return;
                }
                // for(int i=1; i<length; i++) {
                // 	properties[i] = vertex_label+"."+properties[i];
                // }
                
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("\\|");
                    long id = Long.valueOf(values[id_loc]);
                    if(id_map.containsKey(id))
                    	System.out.println("Duplicate key: "+id);
                    id_map.put(id, _global_id++);

                    if(id_map.get(id) % _partition_num != _cur_partition) continue;

                    loaded_num++;
                    long janusVertexId = ((StandardJanusGraph) graph).getIDManager().toVertexId(id_map.get(id));
                    Vertex v = tx.addVertex(T.id, janusVertexId, T.label, vertex_label);
                    
                    for(int i=0; i < properties.length; i++) {
                        if(i == id_loc) continue;
                        if(values[i].isEmpty())
                            continue;
                        String property = properties[i];
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
                    
                    if(count++ == 1000) {
                        tx.commit();
                        count = 0;
                        tx = graph.newTransaction();
                    }

                }
                br.close();
            }
            catch(Exception e) {
            	System.out.println("error: "+e.getMessage());
            }
            if(count != 0) {
                tx.commit();
            }
            System.out.println(" to " + _global_id);
            System.out.println("---- actual number in this partition: " + loaded_num);
        }

    }

    static void load_edges(final JanusGraph graph) {
        get_edge_files();
        for(String file_name : _cur_files) {
            int count =0;
            JanusGraphTransaction tx = graph.newTransaction();

            String relation_name = file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0];
            String[] names = relation_name.split("_");
            String source_label = _name_map.get(names[0]);
            String dest_label = _name_map.get(names[2]);
            String edge_label = names[1];
            System.out.println("Loading "+ source_label+" "+edge_label+" "+dest_label);

            try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
                String line;
                line = br.readLine();
                
                // Initialize edge properties
                String[] properties = line.split("\\|");
                int length = properties.length;
                int src_id_loc = length + 1;
                int dst_id_loc = length + 1;
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

                // for(int i=2; i<length; i++) {
                // 	properties[i] = edge_label+"."+properties[i];
                // }
                
                while ((line = br.readLine()) != null) {
                	String[] values = line.split("\\|");
                    long local_src = Long.valueOf(values[src_id_loc]);
                    long local_dest = Long.valueOf(values[dst_id_loc]);
                    long src_id = _label_ids_map.get(source_label).get(local_src);
                    long dest_id = _label_ids_map.get(dest_label).get(local_dest);

                    if((src_id % _partition_num != _cur_partition) || (dest_id % _partition_num != _cur_partition)) continue;

                    Vertex src = tx.getVertex(((StandardJanusGraph) graph).getIDManager().toVertexId(src_id));
                    Vertex dest = tx.getVertex(((StandardJanusGraph) graph).getIDManager().toVertexId(dest_id));
                    Edge edge = src.addEdge(edge_label, dest);
                    
                    for(int i=0; i<properties.length; i++) {
                        if(i == src_id_loc || i == dst_id_loc) continue;
                        if(values[i].isEmpty())
                    		continue;
                    	String property = properties[i];
                    	if(propertyTypes.get(property).equals("String")) {
                    		edge.property(property, values[i]);
                    	}
                    	else {
                            if(properties[i].equals("creationDate") || properties[i].equals("joinDate")) {
                                edge.property(property, encode_datetime(values[i]));
                            } 
                            else {
                                edge.property(property, Long.valueOf(values[i]));
                            }
                    	}
                    }
                    
                    if(count++ == 1000) {
                        tx.commit();
                        count = 0;
                        tx = graph.newTransaction();
                    }
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error: "+e.getMessage());
            }

            if(count != 0) {
                tx.commit();
            }

        }

    }

    static void load_extra_properties(final JanusGraph graph) {
        get_extra_property_files();
        for(String file_name : _cur_files) {
            int count =0;
            JanusGraphTransaction tx = graph.newTransaction();

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
                            if(dump_vertex % _partition_num == _cur_partition) {
                                Vertex v = tx.getVertex(((StandardJanusGraph) graph).getIDManager().toVertexId(dump_vertex));
                                v.property(vertex_property, props.substring(0, props.length()-1));
                            }
                            props = "";
                        }
                        last_vertex = vertex;
                    }
                    props += (values[2] + " ");

                    if(count++ == 1000) {
                        tx.commit();
                        count = 0;
                        tx = graph.newTransaction();
                    }
                }
                br.close();
            }
            catch(Exception e) {
                System.out.println("error: "+e.getMessage());
            }

            if(count != 0) {
                tx.commit();
            }
        }
    }
    
    static void load_property(final JanusGraph graph) {
    	JanusGraphManagement management = graph.openManagement();
        try (BufferedReader br = new BufferedReader(new FileReader(_poperty_file_path))) {
            String line;
            line = br.readLine();
			while(line != null) {
				String name = line.split(" ")[0];
                String type = line.split(" ")[1];
                System.out.println("Loading property: " + name + "(" + type + ")");
                propertyTypes.put(name, type);
				if(type.equals("String")) {
					management.makePropertyKey(name).dataType(String.class).make();
				}
				else {
					management.makePropertyKey(name).dataType(Long.class).make();
				}
				line = br.readLine();
			}
            br.close();
        }
        catch(Exception e) {
            System.out.println("error: "+e.getMessage());
        }
        management.commit();
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

 public static void configure_partition(long partition_number, long current_partition) {
     _partition_num = partition_number;
     _cur_partition = current_partition;
 }

 public static void dedupe_schema_files() {
    String[] files = { 
        "/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToPropertyName.csv",
        "/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToPropertyNameRemaining.csv",
        "/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToVertexLabel.csv",
        "/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/typeIDToEdgeLabel.csv",
    };
    HashMap<String, Integer> properties_to_id = new HashMap<>();
    HashMap<String, Integer> properties_to_type = new HashMap<>();
	HashMap<String, Integer> labels = new HashMap<>();
	// HashSet<String> contents = new HashSet<>();
    
    
	for(int i=0; i<2; i++) {
        try (BufferedReader br = new BufferedReader(new FileReader(files[i]))) {
            String line = br.readLine();
            while(line != null) {
                if(!line.contains("$")) {
                    String[] values = line.split(",");
                    properties_to_id.put(values[1], Integer.valueOf(values[0]));
                    properties_to_type.put(values[1], Integer.valueOf(values[2]));
                }
                line = br.readLine();
            }
            br.close();
        }
        catch(Exception e) {
            System.out.println("error: "+e.getMessage());
        }
    }
    
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(files[0]))) {
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
        System.out.println("error: "+e.getMessage());
    }
	// System.out.println(contents.size());
	// for(String s : contents) {
	// 	String[] values = s.split(",");
	// 	properties.put(values[1], Integer.valueOf(values[0]));
	// 	System.out.println("{\""+values[1]+"\","+values[0]+"},");
	// }
	
	for(int i=2; i<4; i++) {
        labels.clear();

        try (BufferedReader br = new BufferedReader(new FileReader(files[i]))) {
            String line = br.readLine();
            while(line != null) {
                if(!(line.contains("v[") || line.contains("$") )) {
                    String[] values = line.split(",");
                    labels.put(values[1], Integer.valueOf(values[0]));
                }
                line = br.readLine();
            }
            br.close();
        }
        catch(Exception e) {
            System.out.println("error: "+e.getMessage());
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(files[i]))) {
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
            System.out.println("error: "+e.getMessage());
        }
	}
    

	// System.out.println(contents.size());
	// for(String s : contents) {
	// 	String[] values = s.split(",");
	// 	properties.put(values[1], Integer.valueOf(values[0]));
	// 	System.out.println("{\""+values[1]+"\","+values[0]+"},");
	// }
    
	// System.out.println(contents.size());
	// for(String s : contents) {
	// 	String[] values = s.split(",");
	// 	properties.put(values[1], Integer.valueOf(values[0]));
	// 	System.out.println("{"+values[0]+",\""+values[1]+"\"},");
	// }
 }

 public static void load(final JanusGraph graph) {
    //  _partition_num = partition_number;
    //  _cur_partition = current_partition;

     //Create Schema
     load_helper();
     load_property(graph);

     // Load graph
     load_vertex(graph);
     load_edges(graph);
     load_extra_properties(graph);
     dedupe_schema_files();
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


