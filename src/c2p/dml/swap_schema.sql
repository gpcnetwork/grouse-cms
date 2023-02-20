/*
# Copyright (c) 2021-2025 University of Missouri                   
# Author: Xing Song, xsm7f@umsystem.edu                            
# File: swap_schema.sql                                                 
# Desccription: Snowflake Stored Procedure (SP) for swapping tables between two schemas and 
#               clearing outdated views in current schema
*/

/*swap out CMS_PCORNET_CDM (or sub-tables) with CMS_PCORNET_CDM_BACKUP*/
create or replace procedure swap_schema(TABLE_NAMES array)
returns variant
language javascript
as
$$
/**swap out CMS_PCORNET_CDM (or sub-tables) with CMS_PCORNET_CDM_BACKUP
 * @param {array} TABLE_NAMES: list of table to swap; 'ALL' can be used to swap everything below the two schemas
**/
var i;
for(i=0; i<TABLE_NAMES.length; i++){
    var table = TABLE_NAMES[i].toString();
    let swap_stmt = ``;
    if(table.includes('ALL')){
        swap_stmt += `ALTER SCHEMA CMS_PCORNET_CDM 
                      SWAP WITH CMS_PCORNET_CDM_BACKUP;`;
    }else{
        swap_stmt += `ALTER TABLE CMS_PCORNET_CDM_BACKUP.`+ table +`
                      SWAP WITH CMS_PCORNET_CDM.`+ table +`;`;
    }
    var swap_stmt_run = snowflake.createStatement({sqlText:swap_stmt});
    swap_stmt_run.execute();
}
$$
;

/*clean all views under CMS_PCORNET_CDM_BACKUP*/
create or replace procedure drop_views_from_schema(SCHEMA_NAME string)
returns variant
language javascript
as
$$
/**stage encounter table from different CMS table
 * @param {string} SCHEMA_NAME: schema name for view deletion
**/

// collect views
var collate_tgt_col = snowflake.createStatement({
    sqlText: `SELECT DISTINCT table_name as views
                FROM information_schema.tables 
                WHERE table_type = 'VIEW' AND
                      table_schema = '`+ SCHEMA_NAME +`';`});
var all_views = collate_tgt_col.execute(); 

// run drop statment for each view
while(all_views.next())
{
    // build drop query
    var view_name = all_views.getColumnValue(1);
    var drop_stmt = `DROP VIEW `+ SCHEMA_NAME +`.`+ view_name + `;`;
    
    // run dynamic dml query
    var drop_view = snowflake.createStatement({sqlText: drop_stmt});
    var commit_txn = snowflake.createStatement({sqlText: `commit;`});
    drop_view.execute();
    commit_txn.execute();
}
$$
;
