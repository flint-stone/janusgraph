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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class OptmLdbcPartitionGraphFactory {
    static ArrayList<JanusGraph> _graphs = new ArrayList<JanusGraph>();
	// static HashMap<String, HashMap<Long, Long>>  _label_ids_map = new HashMap<String, HashMap<Long, Long>>();
    static HashMap<String, String> _name_map = new HashMap<>();
    static ArrayList<String> _cur_files = new ArrayList<String>();
    static String _poperty_file_path = "/home/houbai/codelab/documents/LDBC_properties.txt";
    static String _prefix_path = "";
    static String _folder_name = "";
    static ArrayList<String> vertexLabels = new ArrayList<>();
    static HashSet<String> edgeLabels = new HashSet<>();
    static HashSet<Long> vertices_at_other_partitions = new HashSet<>();
    static HashMap<String, String> propertyTypes = new HashMap<>();
    // static HashSet<Long>[] shadow_vertices_at_partitions;
    
    //storage::graph_store* _graph;
    static long _global_id = 1;
    static int _partition_num = 1;
    // static int[] _ptn_num;
    static int _thread_num = 32;
    static int _tx_batch_size = 60000;

    static long get_global_id(String v_label, long oral_id) {
        if(v_label.equals("Comment")) {
            return oral_id + 1000000000000000L;
        }
        else if(v_label.equals("Forum")) {
            return oral_id + 2000000000000000L;
        }
        else if(v_label.equals("Organisation")) {
            return oral_id + 3000000000000000L;
        }
        else if(v_label.equals("Person")) {
            return oral_id + 4000000000000000L;
        }
        else if(v_label.equals("Place")) {
            return oral_id + 5000000000000000L;
        }
        else if(v_label.equals("Post")) {
            return oral_id + 6000000000000000L;
        }
        else if(v_label.equals("Tag")) {
            return oral_id + 7000000000000000L;
        }
        else if(v_label.equals("TagClass")) {
            return oral_id + 8000000000000000L;
        }
        return 1L;
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

        // _label_ids_map.put("Comment", new HashMap<Long, Long>());
        // _label_ids_map.put("Forum", new HashMap<Long, Long>());
        // _label_ids_map.put("Organisation", new HashMap<Long, Long>());
        // _label_ids_map.put("Person", new HashMap<Long, Long>());
        // _label_ids_map.put("Place", new HashMap<Long, Long>());
        // _label_ids_map.put("Post", new HashMap<Long, Long>());
        // _label_ids_map.put("Tag", new HashMap<Long, Long>());
        // _label_ids_map.put("TagClass", new HashMap<Long, Long>());
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

    static void get_vertex_batch_files(int batch_id) {
        _cur_files.clear();
        if(batch_id == 0) {
            _cur_files.add(_prefix_path + "comment_0_0.csv");
        }
        else if(batch_id == 1) {
            _cur_files.add(_prefix_path + "forum_0_0.csv");
            _cur_files.add(_prefix_path + "organisation_0_0.csv");
            _cur_files.add(_prefix_path + "person_0_0.csv");
            _cur_files.add(_prefix_path + "place_0_0.csv");
            _cur_files.add(_prefix_path + "post_0_0.csv");
            _cur_files.add(_prefix_path + "tag_0_0.csv");
            _cur_files.add(_prefix_path + "tagclass_0_0.csv");
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

    static void get_edge_batch_files(int batch_id) {
        _cur_files.clear();
        if(batch_id == 0) {
            _cur_files.add(_prefix_path + "comment_hasCreator_person_0_0.csv");
            _cur_files.add(_prefix_path + "comment_hasTag_tag_0_0.csv");
        }
        else if(batch_id == 1) {
            _cur_files.add(_prefix_path + "comment_isLocatedIn_place_0_0.csv");
            _cur_files.add(_prefix_path + "comment_replyOf_comment_0_0.csv");
            _cur_files.add(_prefix_path + "comment_replyOf_post_0_0.csv");
        }
        else if(batch_id == 2) {
            _cur_files.add(_prefix_path + "forum_containerOf_post_0_0.csv");
            _cur_files.add(_prefix_path + "forum_hasMember_person_0_0.csv");
        }
        else if(batch_id == 3) {
            _cur_files.add(_prefix_path + "forum_hasModerator_person_0_0.csv");
            _cur_files.add(_prefix_path + "forum_hasTag_tag_0_0.csv");
            _cur_files.add(_prefix_path + "organisation_isLocatedIn_place_0_0.csv");
            _cur_files.add(_prefix_path + "person_hasInterest_tag_0_0.csv");
            _cur_files.add(_prefix_path + "person_isLocatedIn_place_0_0.csv");
            _cur_files.add(_prefix_path + "person_knows_person_0_0.csv");
        }
        else if(batch_id == 4) {
            _cur_files.add(_prefix_path + "person_likes_comment_0_0.csv");
        }
        else if(batch_id == 5) {
            _cur_files.add(_prefix_path + "person_likes_post_0_0.csv");
            _cur_files.add(_prefix_path + "person_studyAt_organisation_0_0.csv");
            _cur_files.add(_prefix_path + "person_workAt_organisation_0_0.csv");
            _cur_files.add(_prefix_path + "place_isPartOf_place_0_0.csv");
        }
        else if(batch_id == 6) {
            _cur_files.add(_prefix_path + "post_hasCreator_person_0_0.csv");
            _cur_files.add(_prefix_path + "post_hasTag_tag_0_0.csv");
            _cur_files.add(_prefix_path + "post_isLocatedIn_place_0_0.csv");
            _cur_files.add(_prefix_path + "tag_hasType_tagclass_0_0.csv");
            _cur_files.add(_prefix_path + "tagclass_isSubclassOf_tagclass_0_0.csv");
        }
    }

    static void get_extra_property_files() {
        _cur_files.clear();
        _cur_files.add(_prefix_path + "person_email_emailaddress_0_0.csv");
        _cur_files.add(_prefix_path + "person_speaks_language_0_0.csv");
    }

   static long encode_datetime(String datetime) {
        long epochTime = 0; 
        SimpleDateFormat crunchifyFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS+00:00");
        try {
            Date date = crunchifyFormat.parse(datetime);
            epochTime = date.getTime();
        }
        catch (ParseException e) {
			e.printStackTrace();
		}
        return epochTime;
   }

    static long encode_date(String date) {
        String[] parts = date.split("-");
        long date_value = 0;
        date_value += Long.valueOf(parts[0]); // add year
        date_value = date_value * 100 + Long.valueOf(parts[1]); // add month
        date_value = date_value * 100 + Long.valueOf(parts[2]); // add day
        return date_value;
    }

    static int _benchmark_query_num = 100;

    static ArrayList<String> get_gremlin_query_format() {
        ArrayList<String> formats = new ArrayList<String>();
        formats.add("");
        // q1
        formats.add("g.V(%1$s).repeat(__.out('knows').simplePath()).times(3).emit().dedup()."
                  + "has('firstName',eq('%2$s')).order().by(__.path().count(local)).by('lastName').by('id')."
                  + "project('PersonId', 'PersonLastName', 'DistanceFromPerson').by('id').by('lastName').by(__.path().count(local)).limit(20)");
        // q2
        formats.add("g.V(%1$s).out('knows').in('hasCreator').hasLabel('Post').has('creationDate',lte(%2$s)).order().by('creationDate', decr).by('id').limit(20)");
        // q3
        formats.add("g.V(%1$s).repeat(__.out('knows')).times(2).emit().dedup()."
                  + "filter(__.out('isLocatedIn').out('isPartOf').has('name', without('%4$s', '%5$s')))." 
                  + "filter(__.in('hasCreator').out('isLocatedIn').values('name').filter{it.get().contains('%4$s')}.filter{it.get().contains('%5$s')}).as('friends')."
                  + "project('PersonId', 'xCount', 'yCount').by(select('friends'))."
                  + "by(select('friends').in('hasCreator').has('creationDate',inside(%2$s, %3$s)).filter(out('isLocatedIn').has('name', '%4$s')).count())."
                  + "by(select('friends').in('hasCreator').has('creationDate',inside(%2$s, %3$s)).filter(out('isLocatedIn').has('name', '%5$s')).count())."
                  + "order().by(select('xCount'), decr).by(select('PersonId')).limit(20)");
        // q4
        formats.add("g.V(%1$s).out('knows').store('friends').barrier().in('hasCreator')."
                  + "hasLabel('Post').has('creationDate',inside(%2$s, %3$s))."
                  + "out('hasTag').filter(__.in('hasTag').hasLabel('Post').has('creationDate', lt(%2$s))."
                  + "filter(out('hasCreator').where(within('friends'))).count().is(0)).group().by().by(count()).unfold()."
                  + "order().by(select(values), decr).by(select(keys).values('name'))."
                  + "project('TagName', 'TagCount').by(select(keys).values('name')).by(select(values)).limit(10)");
        // q5
        formats.add("g.V(%1$s).out('knows').union(identity(), out('knows')).dedup().store('friend').barrier()."
                  + "inE('hasMember').has('creationDate', gt(%2$s)).outV().as('forum').dedup()."
                  + "group().by().by(out('containerOf').out('hasCreator').where(within('friend')).count()).unfold()."
                  + "order().by(select(values), decr).by(select(keys)).limit(20)");
        // q6
        formats.add("g.V(%1$s).repeat(__.out('knows')).times(2).emit().dedup().in('hasCreator').hasLabel('Post')."
                  + "filter(out('hasTag').values('name').filter{it.get().contains('%2$s')})."
                  + "out('hasTag').has('name', without('%2$s')).group().by().by(count()).unfold()."
                  + "order().by(select(values), decr).by(select(keys).values('name')).limit(10)");
        // q7
        formats.add("g.V(%1$s).in('hasCreator').as('messages')."
                  + "inE('likes').as('begin_like').outV().as('liker')."
                  + "order().by(select('begin_like').values('creationDate'), decr).by(select('liker'))."
                  + "project('LikePerson', 'LikeDate', 'MsgContent', 'LikeLatency', 'IsFriendFlag')."
                  + "by(select('liker')).by(select('begin_like').values('creationDate'))."
                  + "by(select('messages').values('content')).by(math('begin_like - messages')."
                  + "by('creationDate')).by(select('liker').in('knows').hasId(%1$s).count()).limit(20)");
        // q8
        formats.add("g.V(%1$s).in('hasCreator').in('replyOf').hasLabel('Comment').as('comment')."
                  + "out('hasCreator').as('commenter').order().by(select('comment').values('creationDate'), decr)."
                  + "by(select('comment')).select('commenter','comment').limit(20)");
        // q9
        formats.add("g.V(%1$s).repeat(__.out('knows')).times(2).emit().dedup().as('creators').in('hasCreator')."
                  + "has('creationDate',lt(%2$s)).as('messages')."
                  + "order().by(select('messages').values('creationDate'), decr).by(select('messages'))."
                  + "project('MessageId', 'CreationDate', 'Creator', 'ContentOrImageFile').by(select('messages'))."
                  + "by(select('messages').values('creationDate')).by(select('messages').out('hasCreator'))."
                  + "by(select('messages').values('content', 'imageFile')).limit(20)");
        // q10
        formats.add("g.V(%1$s).out('knows').out('knows').dedup().filter(__.project('b_day', 'b_month')."
                  + "by(math('_ - floor(_ / 100) * 100').by('birthday')).by(math('floor(_ / 100) - floor(_ / 10000) * 100')."
                  + "by('birthday')).or(and(select('b_month').is(eq(%2$s)), select('b_day').is(gte(21))), and(select('b_month').is(eq((%2$s + 1) %% 12)), select('b_day').is(lt(22)))))."
                  + "as('person').project('person_id', 'sum', 'common').by(select('person')).by(select('person')."
                  + "in('hasCreator').count()).by(select('person').in('hasCreator').where(out('hasTag')."
                  + "in('hasInterest').hasLabel('Person').hasId(%1$s)).count()).project('PersonId', 'CommonScore')."
                  + "by(select('person')).by(math('2 * common - sum')).order().by(select('CommonScore'), decr)."
                  + "by(select('PersonId')).limit(10)");
        // q11
        formats.add("g.V(%1$s).repeat(__.out('knows')).emit().times(2).dedup().as('friends')."
                  + "outE('workAt').has('workFrom', lt(%3$s)).as('startWork').inV()."
                  + "filter(out('isLocatedIn').has('name',eq('%2$s'))).as('organ')."
                  + "order().by(select('startWork').values('workFrom')).by(select('friends')).by(select('organ').values('name'), decr)."
                  + "select('friends', 'startWork', 'organ').by().by('workFrom').by('name').limit(10)");
        // q12
        formats.add("g.V(%1$s).out('knows').as('friends').in('hasCreator').hasLabel('Comment').as('replies')."
                  + "out('replyOf').hasLabel('Post').out('hasTag').filter(out('hasType').union(identity(), out('isSubclassOf'))."
                  + "values('name').filter{it.get().contains('%2$s')}).as('tags')."
                  + "group().by(select('friends')).by(group().by(select('replies')).by(select('tags')).unfold()."
                  + "union(select(keys).count(), select(values).dedup().values('name')).fold()).unfold()."
                  + "order().by(select(values).unfold().limit(1), decr).by(select(keys)).limit(20)");
        
        return formats;
    }

    static void convert_query_params() {
        ArrayList<String> gremlin_formats = get_gremlin_query_format();
        SimpleDateFormat crunchifyFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS+00:00");
        String input_path_folder = _prefix_path + "substitution_parameters/";
        String output_path_folder = "/home/houbai/codelab/janusgraph-0.5.0-SNAPSHOT-hadoop2/db/bdbje-partitions/" + _folder_name + "/queries/";
        String line;
        int read_num;
        for(int i = 1; i <= 12; i++) {
            String input_param_list_file = input_path_folder + "interactive_" + i + "_param.txt";
            String brane_output_file = output_path_folder + "brane_interactive_" + i + "_param.txt";
            String janusgraph_output_file = output_path_folder + "janusgraph_interactive_" + i + "_query.txt";
            String neo4j_output_file = output_path_folder + "neo4j_interactive_" + i + "_param.txt";
            try {
                BufferedReader br = new BufferedReader(new FileReader(input_param_list_file));
                BufferedWriter bw_brane = new BufferedWriter(new FileWriter(brane_output_file));
                BufferedWriter bw_janus = new BufferedWriter(new FileWriter(janusgraph_output_file));
                BufferedWriter bw_neo4j = new BufferedWriter(new FileWriter(neo4j_output_file));

                line = br.readLine();
                String[] param_names = line.split("\\|");
                int len = param_names.length;
                for(int x = 0; x < len; x++) {
                    if(param_names[x].equals("durationDays")) {
                        param_names[x] = "endDate";
                    }
                    bw_brane.write(param_names[x]);
                    bw_janus.write(param_names[x]);
                    bw_neo4j.write(param_names[x]);
                    if(x < len -1) {
                        bw_brane.write(" ");
                        bw_janus.write(" ");
                        bw_neo4j.write(" ");
                    }
                }

                read_num = 0;
                while ((line = br.readLine()) != null) {
                    String[] values = line.split("\\|");
                    for(int x = 0; x < len; x++) {
                        if(param_names[x].equals("endDate")) {
                            long epoch_count = Long.valueOf(values[x-1]) + 24 * 60 * 60 * 1000 * Long.valueOf(values[x]);
                            values[x] = String.valueOf(epoch_count);
                        }
                    }

                    bw_neo4j.newLine();
                    for(int x = 0; x < len; x++) {
                        String param_x = values[x];
                        if(param_names[x].endsWith("Date")) {
                            Date param_date = new Date(Long.valueOf(param_x));
                            param_x = crunchifyFormat.format(param_date);
                        }
                        bw_neo4j.write(param_x);
                        if(x < len -1) {
                            bw_neo4j.write(" ");
                        }
                    }

                    bw_brane.newLine();
                    values[0] = String.valueOf(get_global_id("Person", Long.valueOf(values[0])));
                    for(int x = 0; x < len; x++) {
                        bw_brane.write(values[x]);
                        if(x < len -1) {
                            bw_brane.write(" ");
                        }
                    }

                    bw_janus.newLine();
                    values[0] = String.valueOf(Long.valueOf(values[0]) * 256);
                    bw_janus.write(String.format(gremlin_formats.get(i), values));

                    if(++read_num == _benchmark_query_num) break;
                }

                br.close();
                bw_brane.flush();
                bw_brane.close();
                bw_janus.flush();
                bw_janus.close();
                bw_neo4j.flush();
                bw_neo4j.close();
            }
            catch(Exception e) {
                System.out.println("error in convert_query_params: " + e.getMessage());
            }

        }
    }

    static class VtxLoadThread implements Runnable {
        private Thread t = null;
        private int t_id;
        private String vertex_label;
        private int id_loc;
        private String header_line;
        private ArrayList<String> lines = new ArrayList<String>();

        public VtxLoadThread(int id) {
            this.t_id = id;
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

        public void add_line(String l) {
            this.lines.add(l);
        }

        public void run() {
            lines.trimToSize();
            JanusGraphTransaction[] txs = new JanusGraphTransaction[(int)_partition_num];
            TransactionBuilder tbuilder;

            for(int i = 0; i < _partition_num; i++) {
                tbuilder = _graphs.get(i).buildTransaction();
                tbuilder.dirtyVertexSize(500000);
                tbuilder.checkInternalVertexExistence(false);
                tbuilder.checkExternalVertexExistence(false);
                tbuilder.consistencyChecks(false);
                txs[i] = tbuilder.start();
            }

            String[] properties = header_line.split("\\|");

            // String line;
            int read_count = 0;
            for(int idx = 0; idx < lines.size(); idx++) {
                if(++read_count % 100000 == 0) {
                    System.out.println("        [Thread " + t_id + "] processed number : " + read_count);
                }

                String[] values = lines.get(idx).split("\\|");
                long g_id = get_global_id(vertex_label, Long.valueOf(values[id_loc]));
                int partition_id = (int)(g_id % _partition_num);
                // _ptn_num[partition_id]++;
                
                Vertex v = txs[partition_id].addVertex(T.id, ((StandardJanusGraph) _graphs.get(partition_id)).getIDManager().toVertexId(g_id), 
                                                       T.label, vertex_label);
                for(int i = 0; i < properties.length; i++) {
                    if(propertyTypes.get(properties[i]).equals("String")) {
                        v.property(properties[i], values[i]);
                    }
                    else {
                        if(properties[i].equals("creationDate") || properties[i].equals("joinDate")) {
                            v.property(properties[i], encode_datetime(values[i]));
                        } 
                        else if(properties[i].equals("birthday")) {
                            v.property(properties[i], encode_date(values[i]));
                        }
                        else {
                            v.property(properties[i], Long.valueOf(values[i]));
                        }
                    }
                }

                values = null;
                v = null;
            }

            for(int i = 0; i < _partition_num; i++) {
                txs[i].commit();
                txs[i] = null;
            }

            txs = null;
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
        }
    }

   static void load_vertex(int batch_id) {
       get_vertex_batch_files(batch_id);
       VtxLoadThread[] loaders = new VtxLoadThread[_thread_num];

       for(String file_name : _cur_files) {
            // Create thread loaders
            for(int i = 0; i < _thread_num; i++) {
                loaders[i] = new VtxLoadThread(i);
            }

            // Parse vertex label
            String vertex_label = _name_map.get(file_name.split("/" + _folder_name + "/")[1].split("_0_0")[0]);
            for(int i = 0; i < _thread_num; i++) {
                loaders[i].set_vertex_label(vertex_label);
            }
            System.out.println("Loading vertex: "+ vertex_label);
            
            // Read files
            try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
                // Read header line
                String header_line = br.readLine();
                String[] properties = header_line.split("\\|");
                int id_loc = properties.length + 1;
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
                    loaders[i].set_header_line(header_line);
                    loaders[i].set_id_loc(id_loc);
                }

                // Store lines in file
                int line_readed = 0;
                String line;
                while ((line = br.readLine()) != null) {
                    loaders[line_readed++ % _thread_num].add_line(line);
                    if(line_readed % 1000000 == 0)
                        System.out.println("        CSV records read number : " + line_readed);

                    if(line_readed % 40000000 == 0) {
                        for(int i = 0; i < _thread_num; i++) {
                            loaders[i].start();
                        }
                        for(int i = 0; i < _thread_num; i++) {
                            loaders[i].join();
                        }
                        for(int i = 0; i < _thread_num; i++) {
                            loaders[i] = new VtxLoadThread(i);
                            loaders[i].set_vertex_label(vertex_label);
                            loaders[i].set_header_line(header_line);
                            loaders[i].set_id_loc(id_loc);
                        }
                        System.gc();
                        System.runFinalization();
                    }
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
        }

        // for(int i = 0; i < _partition_num; i++) {
        //     System.out.println("Partition " + i + " vertex number: " + _ptn_num[i]);
        // }
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

        public ShadowVtxLoadThread(int id) {
            this.t_id = id;
        }

        public void add_insert_unit(int partition, long shadow_vertex_id, String shadow_vertex_label) {
            this.lines.add(new shadow_insert_unit(partition, shadow_vertex_id, shadow_vertex_label));
        }

        public void run() {
            lines.trimToSize();
            JanusGraphTransaction[] txs = new JanusGraphTransaction[(int)_partition_num];
            TransactionBuilder tbuilder;

            for(int i = 0; i < _partition_num; i++) {
                tbuilder = _graphs.get(i).buildTransaction();
                tbuilder.dirtyVertexSize(1000000);
                tbuilder.checkInternalVertexExistence(false);
                tbuilder.checkExternalVertexExistence(false);
                tbuilder.consistencyChecks(false);
                txs[i] = tbuilder.start();
            }

            int read_count = 0;
            for(int idx = 0; idx < lines.size(); idx++) {
                if(++read_count % 100000 == 0)
                    System.out.println("        [Thread " + t_id + "] Loading shadow vertices, processed number : " + read_count);
                
                int partition_id = lines.get(idx).p_id;
                txs[partition_id].addVertex(T.id, lines.get(idx).shadow_v_id, T.label, lines.get(idx).shadow_v_label);
            }

            for(int i = 0; i < _partition_num; i++) {
                txs[i].commit();
                txs[i] = null;
            }

            txs = null;
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
        }
    }

    static void load_shadow_vertices(int batch_id) {
        System.out.println("Loading shadow vertices of bacth " + batch_id);
        String file_path = "/home/houbai/codelab/documents/data/" + _folder_name + "/shadow_insert_files/" 
                      + _partition_num + "/shadow_batch_" + batch_id;

        // Create thread loaders
        ShadowVtxLoadThread[] shadow_loaders = new ShadowVtxLoadThread[_thread_num];
        for(int i = 0; i < _thread_num; i++) {
            shadow_loaders[i] = new ShadowVtxLoadThread(i);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file_path))) {
            String line;
            int line_readed = 0;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(" ");
                int partition_id = Integer.valueOf(values[0]);
                long janus_shadow_id_in_other_partition = ((StandardJanusGraph) _graphs.get(partition_id)).getIDManager().toVertexId(Long.valueOf(values[1]));
                shadow_loaders[line_readed++ % _thread_num].add_insert_unit(
                    partition_id, janus_shadow_id_in_other_partition, values[2]);

                if(line_readed % 1000000 == 0)
                    System.out.println("        CSV records read number : " + line_readed);  
            }
            br.close();
        }
        catch(Exception e) {
            System.out.println("error in load_shadow_vertices: "+e.getMessage());
        }

        for(int i = 0; i < _thread_num; i++) {
            shadow_loaders[i].start();
        }
        for(int i = 0; i < _thread_num; i++) {
            shadow_loaders[i].join();
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
            TransactionBuilder tbuilder;

            for(int i = 0; i < _partition_num; i++) {
                tbuilder = _graphs.get(i).buildTransaction();
                tbuilder.dirtyVertexSize(1000000);
                tbuilder.checkInternalVertexExistence(false);
                tbuilder.checkExternalVertexExistence(false);
                tbuilder.consistencyChecks(false);
                txs[i] = tbuilder.start();
            }

            String[] properties = header_line.split("\\|");
            int read_count = 0;
            Edge e1 = null;
            Edge e2 = null;
            for(int idx = 0; idx < lines.size(); idx++) {
                if(++read_count % 100000 == 0)
                    System.out.println("        [Thread " + t_id + "] Loading edges, processed number : " + read_count);

                String[] values = lines.get(idx).split("\\|");
                long global_src = get_global_id(source_label, Long.valueOf(values[src_id_loc]));
                long global_dest = get_global_id(dest_label, Long.valueOf(values[dst_id_loc]));
                int src_partition_id = (int) (global_src % _partition_num);
                int dest_partition_id = (int) (global_dest % _partition_num);
                Vertex src = txs[src_partition_id].getVertex(((StandardJanusGraph) _graphs.get(src_partition_id)).getIDManager().toVertexId(global_src));
                Vertex dest = txs[dest_partition_id].getVertex(((StandardJanusGraph) _graphs.get(dest_partition_id)).getIDManager().toVertexId(global_dest));

                if(src_partition_id == dest_partition_id) {
                    e1 = src.addEdge(edge_label, dest);
                } else {
                    long janus_shadow_src_id_in_dest_partition = ((StandardJanusGraph) _graphs.get(dest_partition_id)).getIDManager().toVertexId(global_src);
                    long janus_shadow_dest_id_in_src_partition = ((StandardJanusGraph) _graphs.get(src_partition_id)).getIDManager().toVertexId(global_dest);
                    Vertex shadow_dest = txs[src_partition_id].getVertex(janus_shadow_dest_id_in_src_partition);
                    Vertex shadow_src = txs[dest_partition_id].getVertex(janus_shadow_src_id_in_dest_partition);
                    e1 = src.addEdge(edge_label, shadow_dest);
                    e2 = shadow_src.addEdge(edge_label, dest);
                }

                for(int i=0; i<properties.length; i++) {
                    if(i == src_id_loc || i == dst_id_loc) continue;
                    if(propertyTypes.get(properties[i]).equals("String")) {
                        if(e1 != null) {
                            e1.property(properties[i], values[i]);
                        }
                        if(e2 != null) {
                            e2.property(properties[i], values[i]);
                        }
                    }
                    else {
                        if(properties[i].equals("creationDate") || properties[i].equals("joinDate")) {
                            if(e1 != null) {
                                e1.property(properties[i], encode_datetime(values[i]));
                            }
                            if(e2 != null) {
                                e2.property(properties[i], encode_datetime(values[i]));
                            }
                        } 
                        else {
                            if(e1 != null) {
                                e1.property(properties[i], Long.valueOf(values[i]));
                            }
                            if(e2 != null) {
                                e2.property(properties[i], Long.valueOf(values[i]));
                            }
                        }
                    }
                }

                e1 = null;
                e2 = null;
            }

            for(int i = 0; i < _partition_num; i++) {
                txs[i].commit();
                txs[i] = null;
            }

            txs = null;
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

    static void load_edges(int batch_id) {
        System.out.println("----------------- Loading edge batch " + batch_id + " -----------------");
        String file_path = "/home/houbai/codelab/documents/data/" + _folder_name + "/cut_edge_files/edge_batch_" + batch_id;

        // Create thread loaders
        EdgeLoadThread[] loaders = new EdgeLoadThread[_thread_num];
        for(int i = 0; i < _thread_num; i++) {
            loaders[i] = new EdgeLoadThread(i);
        }

        try (BufferedReader br = new BufferedReader(new FileReader(file_path))) {
            String line = br.readLine();
            if(!line.startsWith("#flag#")) {
                System.out.println("Error in edge cut.");
            }
            String header_line = br.readLine();
            String[] labels = line.split(" ");
            String source_label = labels[1];
            String edge_label = labels[2];
            String dest_label = labels[3];
            System.out.println("Loading "+ source_label+" "+edge_label+" "+dest_label);
            for(int i = 0; i < _thread_num; i++) {
                loaders[i].set_labels(source_label, edge_label, dest_label);
                loaders[i].set_header_line(header_line);
                loaders[i].set_id_locs(0, 1);
            }

            int line_readed = 0;
            while ((line = br.readLine()) != null) {
                if(line.startsWith("#flag#")) {
                    for(int i = 0; i < _thread_num; i++) {
                        loaders[i].start();
                    }
                    for(int i = 0; i < _thread_num; i++) {
                        loaders[i].join();
                    }

                    labels = line.split(" ");
                    source_label = labels[1];
                    edge_label = labels[2];
                    dest_label = labels[3];
                    header_line = br.readLine();
                    System.out.println("Loading "+ source_label+" "+edge_label+" "+dest_label);
                    for(int i = 0; i < _thread_num; i++) {
                        loaders[i] = new EdgeLoadThread(i);
                        loaders[i].set_labels(source_label, edge_label, dest_label);
                        loaders[i].set_header_line(header_line);
                        loaders[i].set_id_locs(0, 1);
                    }
                } else {
                    loaders[line_readed++ % _thread_num].add_line(line);

                    if(line_readed % 1000000 == 0)
                        System.out.println("        CSV records read number : " + line_readed);
                }
            }
            br.close();
        }
        catch(Exception e) {
            System.out.println("error in load_edges: "+e.getMessage());
        }

        for(int i = 0; i < _thread_num; i++) {
            loaders[i].start();
        }
        for(int i = 0; i < _thread_num; i++) {
            loaders[i].join();
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

    public static void load(final JanusGraph graph) {
    }

    public static void load_graph_vertices(String folder_name, int partition_number, int batch_id) {
        // Init graphs
        _partition_num = partition_number;
        _folder_name = folder_name;
        _prefix_path = "/home/houbai/codelab/documents/data/" + _folder_name + "/";
        load_helper();

        for(int i = 0; i < _partition_num; i++) {
            _graphs.add(i, JanusGraphFactory.open("conf/berkeleyje-partition/janusgraph-berkeleyje-p" + i + ".properties"));
        }
        load_property_types();
        if(batch_id == 0) {
            make_label_schema();
            make_property_schema();
        }

        // Load vertex
        load_vertex(batch_id);
        if(batch_id == 1) {
            convert_query_params();
        }

        for(int i = 0; i < _partition_num; i++) {
            _graphs.get(i).close();
        }
    }

    public static void load_graph_shadow_vertices(String folder_name, int partition_number, int batch_id) {
        if(partition_number == 1) return;

        // Init graphs
        _partition_num = partition_number;
        _folder_name = folder_name;
        _prefix_path = "/home/houbai/codelab/documents/data/" + _folder_name + "/";
        load_helper();

        for(int i = 0; i < _partition_num; i++) {
            _graphs.add(i, JanusGraphFactory.open("conf/berkeleyje-partition/janusgraph-berkeleyje-p" + i + ".properties"));
        }
        load_property_types();

        // Load shadow vertices
        load_shadow_vertices(batch_id);

        for(int i = 0; i < _partition_num; i++) {
            _graphs.get(i).close();
        }
    }

    public static void load_graph_edges(String folder_name, int partition_number, int batch_id) {
        // Init graphs
        _partition_num = partition_number;
        _folder_name = folder_name;
        _prefix_path = "/home/houbai/codelab/documents/data/" + _folder_name + "/";
        load_helper();

        for(int i = 0; i < _partition_num; i++) {
            _graphs.add(i, JanusGraphFactory.open("conf/berkeleyje-partition/janusgraph-berkeleyje-p" + i + ".properties"));
        }
        load_property_types();

        // Load edges
        load_edges(batch_id);

        for(int i = 0; i < _partition_num; i++) {
            _graphs.get(i).close();
        }
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


