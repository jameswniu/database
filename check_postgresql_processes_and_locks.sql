/*Check Active*/
select pid, state, application_name, query, state_change, query_start,  backend_start from pg_stat_activity where usename = 'james_niu_u' and state = 'active' and pid != pg_backend_pid();

/*Check Idle*/
select pid, state, application_name, query, state_change, query_start,  backend_start from pg_stat_activity where usename = 'james_niu_u' and pid != pg_backend_pid()

/*Check All Database Usage*/
select * from pg_stat_activity

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

/*Check Locks not Granted to Write/Edit/Remove Tables*/
SELECT relation::regclass, * from pg_locks --where not granted;

/*Check Which Query Locks Which Other Query*/
select pid, 
       usename, 
       pg_blocking_pids(pid) as blocked_by, 
       query as blocked_query
from pg_stat_activity
--where cardinality(pg_blocking_pids(pid)) > 0;

/*Other Check Locks Script*/
select
   b.pid     as blocked_pid,
   bq.usename  as blocked_user,
   bq.xact_start,
   bq.query_start,
   --bq.wait_event_type,
   --bq.wait_event,
   bq.query    as blocked_query,
   bq.application_name as blocked_application,
   '|' "|",
   h.pid     as blocking_pid,
   hq.usename as blocking_user,
   hq.xact_start,
   hq.query_start,
   hq.query   as blocking_query,
   hq.application_name as blocking_application
from
   pg_catalog.pg_locks b
   join pg_catalog.pg_stat_activity bq on bq.pid = b.pid
   join pg_catalog.pg_locks h
      on
         h.locktype = b.locktype AND
         h.database is not distinct from b.database AND
         h.relation is not distinct from b.relation AND
         h.page is not distinct from b.page AND
         h.tuple is not distinct from b.tuple AND
         h.virtualxid is not distinct from b.virtualxid AND
         h.transactionid is not distinct from b.transactionid AND
         h.classid is not distinct from b.classid AND
         h.objid is not distinct from b.objid AND
         h.objsubid is not distinct from b.objsubid AND
         h.pid != b.pid
   join pg_catalog.pg_stat_activity hq on hq.pid = h.pid
where not b.granted