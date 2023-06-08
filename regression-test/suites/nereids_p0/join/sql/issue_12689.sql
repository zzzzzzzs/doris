--- Nereids does't support array function

--- set enable_nereids_planner=true;
--- set enable_fallback_to_original_planner=false;

--- select xs.plat_id,xs.company_id, xs_email, xs_group_id, kf_email, kf_group_id from (select plat_id, company_id, concat_ws(",", array_sort(collect_set(`user_email`))) xs_email, concat_ws(",", array_sort(collect_set(`group_id`))) xs_group_id from table_3 where plat_id = 1 and is_delete = 0 group by plat_id, company_id) xs left join (select plat_id, company_id, concat_ws(",", array_sort(collect_set(`user_email`))) kf_email, concat_ws(",", array_sort(collect_set(`group_id`))) kf_group_id from table_3 where plat_id = 1 and is_delete = 0 group by plat_id, company_id) kf on xs.plat_id = kf.plat_id and xs.company_id = kf.company_id;
