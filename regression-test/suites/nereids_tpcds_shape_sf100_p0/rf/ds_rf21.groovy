/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

suite("ds_rf21") {
    String db = context.config.getDbNameByFile(new File(context.file.parent))
    sql "use ${db}"
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set exec_mem_limit=21G'
    sql 'set be_number_for_test=3'
    sql 'set parallel_fragment_exec_instance_num=8; '
    sql 'set parallel_pipeline_task_num=8; '
    sql 'set forbid_unknown_col_stats=true'
    sql 'set broadcast_row_count_limit = 30000000'
    sql 'set enable_nereids_timeout = false'
    sql 'set enable_pipeline_engine=true'
    String stmt = '''
    explain physical plan
    select  *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('2002-02-27' as date))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('2002-02-27' as date))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from inventory
       ,warehouse
       ,item
       ,date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between (cast ('2002-02-27' as date) - interval 30 day)
                    and (cast ('2002-02-27' as date) + interval 30 day)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 limit 100;

    '''
    String plan = sql "${stmt}"
    log.info(plan)
    def getRuntimeFilters = { plantree ->
        {
            def lst = []
            plantree.eachMatch("RF\\d+\\[[^#]+#\\d+->\\[[^\\]]+\\]") {
                ch ->
                    {
                        lst.add(ch.replaceAll("#\\d+", ''))
                    }
            }
            return lst.join(',')
        }
    }
    
    // def outFile = "regression-test/suites/nereids_tpcds_shape_sf100_p0/ddl/rf/rf.21"
    // File file = new File(outFile)
    // file.write(getRuntimeFilters(plan))
    
     assertEquals("RF2[w_warehouse_sk->[inv_warehouse_sk],RF1[d_date_sk->[inv_date_sk],RF0[i_item_sk->[inv_item_sk]", getRuntimeFilters(plan))
}
