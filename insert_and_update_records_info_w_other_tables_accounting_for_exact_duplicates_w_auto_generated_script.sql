insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2465133, 3448502, '299713958/60', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2465133
        and src_sk = 3448502
        and pat_acct = '299713958/60'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2465133
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2465133, 3448502, '299713958/60', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2465133
        and src_sk = 3448502
        and pat_acct = '299713958/60'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2465133
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2465133, 3448502, '299713958/60', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2465133
        and src_sk = 3448502
        and pat_acct = '299713958/60'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2465133
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2362203, 3345572, '6671519', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2362203
        and src_sk = 3345572
        and pat_acct = '6671519'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2362203
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2482183, 3465552, '14924429', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2482183
        and src_sk = 3465552
        and pat_acct = '14924429'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2482183
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2482183, 3465552, '14924429', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2482183
        and src_sk = 3465552
        and pat_acct = '14924429'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2482183
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2482183, 3465552, '14924429', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2482183
        and src_sk = 3465552
        and pat_acct = '14924429'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2482183
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2482183, 3465552, '14924429', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2482183
        and src_sk = 3465552
        and pat_acct = '14924429'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2482183
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2483983, 3467352, '21500489843', 'A7:255 - missing or wrong diagnosis code (ICD code)', '277-A7:255'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2483983
        and src_sk = 3467352
        and pat_acct = '21500489843'
        and note = 'A7:255 - missing or wrong diagnosis code (ICD code)'
        and code = '277-A7:255');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2483983
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2483982, 3467351, '21500480100', 'A7:255 - missing or wrong diagnosis code (ICD code)', '277-A7:255'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2483982
        and src_sk = 3467351
        and pat_acct = '21500480100'
        and note = 'A7:255 - missing or wrong diagnosis code (ICD code)'
        and code = '277-A7:255');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2483982
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2483983, 3467352, '21500489843', 'A7:255 - missing or wrong diagnosis code (ICD code)', '277-A7:255'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2483983
        and src_sk = 3467352
        and pat_acct = '21500489843'
        and note = 'A7:255 - missing or wrong diagnosis code (ICD code)'
        and code = '277-A7:255');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2483983
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2483982, 3467351, '21500480100', 'A7:255 - missing or wrong diagnosis code (ICD code)', '277-A7:255'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2483982
        and src_sk = 3467351
        and pat_acct = '21500480100'
        and note = 'A7:255 - missing or wrong diagnosis code (ICD code)'
        and code = '277-A7:255');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2483982
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464942, 3448311, '299700352/127', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464942
        and src_sk = 3448311
        and pat_acct = '299700352/127'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464942
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464975, 3448344, '299781174/411', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464975
        and src_sk = 3448344
        and pat_acct = '299781174/411'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464975
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475068, 3458437, '8349788', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475068
        and src_sk = 3458437
        and pat_acct = '8349788'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475068
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464942, 3448311, '299700352/127', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464942
        and src_sk = 3448311
        and pat_acct = '299700352/127'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464942
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464975, 3448344, '299781174/411', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464975
        and src_sk = 3448344
        and pat_acct = '299781174/411'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464975
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475068, 3458437, '8349788', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475068
        and src_sk = 3458437
        and pat_acct = '8349788'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475068
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464942, 3448311, '299700352/127', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464942
        and src_sk = 3448311
        and pat_acct = '299700352/127'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464942
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464975, 3448344, '299781174/411', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464975
        and src_sk = 3448344
        and pat_acct = '299781174/411'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464975
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475068, 3458437, '8349788', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475068
        and src_sk = 3458437
        and pat_acct = '8349788'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475068
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464942, 3448311, '299700352/127', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464942
        and src_sk = 3448311
        and pat_acct = '299700352/127'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464942
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464975, 3448344, '299781174/411', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464975
        and src_sk = 3448344
        and pat_acct = '299781174/411'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464975
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475068, 3458437, '8349788', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475068
        and src_sk = 3458437
        and pat_acct = '8349788'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475068
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464942, 3448311, '299700352/127', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464942
        and src_sk = 3448311
        and pat_acct = '299700352/127'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464942
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2464975, 3448344, '299781174/411', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2464975
        and src_sk = 3448344
        and pat_acct = '299781174/411'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2464975
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475068, 3458437, '8349788', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475068
        and src_sk = 3458437
        and pat_acct = '8349788'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475068
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476716, 3460085, '300794603/130', 'A6:728 - missing accident state', '277-A6:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476716
        and src_sk = 3460085
        and pat_acct = '300794603/130'
        and note = 'A6:728 - missing accident state'
        and code = '277-A6:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476716
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2343720, 3327089, '8420002', 'A7:84 - service not authorized ?', '277-A7:84'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2343720
        and src_sk = 3327089
        and pat_acct = '8420002'
        and note = 'A7:84 - service not authorized ?'
        and code = '277-A7:84');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2343720
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2465133, 3448502, '299713958/60', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2465133
        and src_sk = 3448502
        and pat_acct = '299713958/60'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2465133
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2465133, 3448502, '299713958/60', 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN', '277-A6:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2465133
        and src_sk = 3448502
        and pat_acct = '299713958/60'
        and note = 'A6:143:82 - Rendering Provider state license missing if no NPI/SSN'
        and code = '277-A6:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2465133
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475599, 3458968, '48500017254', 'A7:145:82 - invalid rendering provider taxonomy code', '277-A7:145:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475599
        and src_sk = 3458968
        and pat_acct = '48500017254'
        and note = 'A7:145:82 - invalid rendering provider taxonomy code'
        and code = '277-A7:145:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475599
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475504, 3458873, '48500017254', 'A7:145:82 - invalid rendering provider taxonomy code', '277-A7:145:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475504
        and src_sk = 3458873
        and pat_acct = '48500017254'
        and note = 'A7:145:82 - invalid rendering provider taxonomy code'
        and code = '277-A7:145:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475504
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475591, 3458960, '48300011813', 'A7:145:82 - invalid rendering provider taxonomy code', '277-A7:145:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475591
        and src_sk = 3458960
        and pat_acct = '48300011813'
        and note = 'A7:145:82 - invalid rendering provider taxonomy code'
        and code = '277-A7:145:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475591
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476226, 3459595, '7300419', 'A7:578 - bad insurance type code', '277-A7:578'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476226
        and src_sk = 3459595
        and pat_acct = '7300419'
        and note = 'A7:578 - bad insurance type code'
        and code = '277-A7:578');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476226
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476290, 3459659, '7289278', 'A7:578 - bad insurance type code', '277-A7:578'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476290
        and src_sk = 3459659
        and pat_acct = '7289278'
        and note = 'A7:578 - bad insurance type code'
        and code = '277-A7:578');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476290
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476226, 3459595, '7300419', 'A7:578 - bad insurance type code', '277-A7:578'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476226
        and src_sk = 3459595
        and pat_acct = '7300419'
        and note = 'A7:578 - bad insurance type code'
        and code = '277-A7:578');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476226
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476290, 3459659, '7289278', 'A7:578 - bad insurance type code', '277-A7:578'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476290
        and src_sk = 3459659
        and pat_acct = '7289278'
        and note = 'A7:578 - bad insurance type code'
        and code = '277-A7:578');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476290
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476226, 3459595, '7300419', 'A7:578 - bad insurance type code', '277-A7:578'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476226
        and src_sk = 3459595
        and pat_acct = '7300419'
        and note = 'A7:578 - bad insurance type code'
        and code = '277-A7:578');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476226
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476290, 3459659, '7289278', 'A7:578 - bad insurance type code', '277-A7:578'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476290
        and src_sk = 3459659
        and pat_acct = '7289278'
        and note = 'A7:578 - bad insurance type code'
        and code = '277-A7:578');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476290
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475068, 3458437, '8349788', 'A7:728 - payer not accepting bills for this state through clearing house', '277-A7:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475068
        and src_sk = 3458437
        and pat_acct = '8349788'
        and note = 'A7:728 - payer not accepting bills for this state through clearing house'
        and code = '277-A7:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475068
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2475068, 3458437, '8349788', 'A7:728 - payer not accepting bills for this state through clearing house', '277-A7:728'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2475068
        and src_sk = 3458437
        and pat_acct = '8349788'
        and note = 'A7:728 - payer not accepting bills for this state through clearing house'
        and code = '277-A7:728');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2475068
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474404, 3457773, '300004792/60', 'A7:143:82 - rejected due to missing doctor state license number', '277-A7:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474404
        and src_sk = 3457773
        and pat_acct = '300004792/60'
        and note = 'A7:143:82 - rejected due to missing doctor state license number'
        and code = '277-A7:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474404
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474404, 3457773, '300004792/60', 'A7:143:82 - rejected due to missing doctor state license number', '277-A7:143:82'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474404
        and src_sk = 3457773
        and pat_acct = '300004792/60'
        and note = 'A7:143:82 - rejected due to missing doctor state license number'
        and code = '277-A7:143:82');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474404
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2470342, 3453711, '14814563', 'A7:500:IL - insured zip must be 5 or 9 characters', '277-A7:500:IL'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2470342
        and src_sk = 3453711
        and pat_acct = '14814563'
        and note = 'A7:500:IL - insured zip must be 5 or 9 characters'
        and code = '277-A7:500:IL');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2470342
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476716, 3460085, '300794603/130', 'PO:629 - pending for Property Casualty Claim Number', '277-PO:629'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476716
        and src_sk = 3460085
        and pat_acct = '300794603/130'
        and note = 'PO:629 - pending for Property Casualty Claim Number'
        and code = '277-PO:629');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'MLX Yi James'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476716
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474357, 3457726, '8404761', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474357
        and src_sk = 3457726
        and pat_acct = '8404761'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474357
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474125, 3457494, '300600473/134', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474125
        and src_sk = 3457494
        and pat_acct = '300600473/134'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474125
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2472739, 3456108, '0009918121413', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2472739
        and src_sk = 3456108
        and pat_acct = '0009918121413'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2472739
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474600, 3457969, '8367209', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474600
        and src_sk = 3457969
        and pat_acct = '8367209'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474600
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474460, 3457829, '8388874', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474460
        and src_sk = 3457829
        and pat_acct = '8388874'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474460
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474457, 3457826, '8367218', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474457
        and src_sk = 3457826
        and pat_acct = '8367218'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474457
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476265, 3459634, '7449786', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476265
        and src_sk = 3459634
        and pat_acct = '7449786'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476265
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476198, 3459567, '279677323/37', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476198
        and src_sk = 3459567
        and pat_acct = '279677323/37'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476198
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2472729, 3456098, '0009918121413', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2472729
        and src_sk = 3456098
        and pat_acct = '0009918121413'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2472729
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2472795, 3456164, '14839998', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2472795
        and src_sk = 3456164
        and pat_acct = '14839998'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2472795
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2477005, 3460374, '8411068', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2477005
        and src_sk = 3460374
        and pat_acct = '8411068'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2477005
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2477108, 3460477, '8426893', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2477108
        and src_sk = 3460477
        and pat_acct = '8426893'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2477108
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2476936, 3460305, '8404819', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2476936
        and src_sk = 3460305
        and pat_acct = '8404819'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2476936
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474451, 3457820, '0032731931400', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474451
        and src_sk = 3457820
        and pat_acct = '0032731931400'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474451
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2474772, 3458141, '0008612431421', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2474772
        and src_sk = 3458141
        and pat_acct = '0008612431421'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2474772
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2477101, 3460470, '8415436', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2477101
        and src_sk = 3460470
        and pat_acct = '8415436'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2477101
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2477814, 3461183, '7763520', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2477814
        and src_sk = 3461183
        and pat_acct = '7763520'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2477814
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2477319, 3460688, '300903371/164', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2477319
        and src_sk = 3460688
        and pat_acct = '300903371/164'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2477319
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2478949, 3462318, '8449514', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2478949
        and src_sk = 3462318
        and pat_acct = '8449514'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2478949
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2479392, 3462761, '301040427/400', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2479392
        and src_sk = 3462761
        and pat_acct = '301040427/400'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2479392
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2479311, 3462680, '301047986/110', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2479311
        and src_sk = 3462680
        and pat_acct = '301047986/110'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2479311
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2479310, 3462679, '301040423/400', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2479310
        and src_sk = 3462679
        and pat_acct = '301040423/400'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2479310
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2482087, 3465456, '8491347', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2482087
        and src_sk = 3465456
        and pat_acct = '8491347'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2482087
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2480605, 3463974, '14870873', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2480605
        and src_sk = 3463974
        and pat_acct = '14870873'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2480605
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2478186, 3461555, '300940583/110', 'A7:0 - please contact payer for more info', '277-A7:0'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2478186
        and src_sk = 3461555
        and pat_acct = '300940583/110'
        and note = 'A7:0 - please contact payer for more info'
        and code = '277-A7:0');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Payer followup MGT'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2478186
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2363833, 3347202, '5939066', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2363833
        and src_sk = 3347202
        and pat_acct = '5939066'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2363833
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2364529, 3347898, '13905656', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2364529
        and src_sk = 3347898
        and pat_acct = '13905656'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2364529
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365101, 3348470, '6978224', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365101
        and src_sk = 3348470
        and pat_acct = '6978224'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365101
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365071, 3348440, '6978245', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365071
        and src_sk = 3348440
        and pat_acct = '6978245'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365071
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365805, 3349174, '7259582', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365805
        and src_sk = 3349174
        and pat_acct = '7259582'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365805
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2363567, 3346936, '5939066', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2363567
        and src_sk = 3346936
        and pat_acct = '5939066'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2363567
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365091, 3348460, '6978224', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365091
        and src_sk = 3348460
        and pat_acct = '6978224'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365091
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365388, 3348757, '7027613', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365388
        and src_sk = 3348757
        and pat_acct = '7027613'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365388
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2364949, 3348318, '6342852', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2364949
        and src_sk = 3348318
        and pat_acct = '6342852'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2364949
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365888, 3349257, '7091774', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365888
        and src_sk = 3349257
        and pat_acct = '7091774'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365888
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2434453, 3417822, '296814478/410', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2434453
        and src_sk = 3417822
        and pat_acct = '296814478/410'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2434453
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2358986, 3342355, '13330318', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2358986
        and src_sk = 3342355
        and pat_acct = '13330318'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2358986
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2119490, 3103288, '21800223171', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2119490
        and src_sk = 3103288
        and pat_acct = '21800223171'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2119490
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2097429, 3081227, '21800168745', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2097429
        and src_sk = 3081227
        and pat_acct = '21800168745'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2097429
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2362203, 3345572, '6671519', 'R4:294 - request additional supporting document', '277-R4:294'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2362203
        and src_sk = 3345572
        and pat_acct = '6671519'
        and note = 'R4:294 - request additional supporting document'
        and code = '277-R4:294');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2362203
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2378206, 3361575, '295303727/147', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2378206
        and src_sk = 3361575
        and pat_acct = '295303727/147'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2378206
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2378347, 3361716, '295304056/147', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2378347
        and src_sk = 3361716
        and pat_acct = '295304056/147'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2378347
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2378206, 3361575, '295303727/147', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2378206
        and src_sk = 3361575
        and pat_acct = '295303727/147'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2378206
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2378347, 3361716, '295304056/147', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2378347
        and src_sk = 3361716
        and pat_acct = '295304056/147'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2378347
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2363833, 3347202, '5939066', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2363833
        and src_sk = 3347202
        and pat_acct = '5939066'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2363833
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2364529, 3347898, '13905656', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2364529
        and src_sk = 3347898
        and pat_acct = '13905656'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2364529
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365101, 3348470, '6978224', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365101
        and src_sk = 3348470
        and pat_acct = '6978224'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365101
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365071, 3348440, '6978245', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365071
        and src_sk = 3348440
        and pat_acct = '6978245'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365071
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365805, 3349174, '7259582', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365805
        and src_sk = 3349174
        and pat_acct = '7259582'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365805
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2363567, 3346936, '5939066', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2363567
        and src_sk = 3346936
        and pat_acct = '5939066'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2363567
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365091, 3348460, '6978224', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365091
        and src_sk = 3348460
        and pat_acct = '6978224'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365091
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2365388, 3348757, '7027613', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2365388
        and src_sk = 3348757
        and pat_acct = '7027613'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2365388
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2434453, 3417822, '296814478/410', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2434453
        and src_sk = 3417822
        and pat_acct = '296814478/410'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2434453
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2358986, 3342355, '13330318', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2358986
        and src_sk = 3342355
        and pat_acct = '13330318'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2358986
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2119490, 3103288, '21800223171', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2119490
        and src_sk = 3103288
        and pat_acct = '21800223171'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2119490
      and a.processed is null;

insert into
      business_analysis.jopari_claim_alerts (created_at, pm_sk, src_sk, pat_acct, note, code) 
select
      now(), 2097429, 3081227, '21800168745', 'P3:95 - pending for additional information not received', '277-P3:95'
where not exists (
    select 1 from business_analysis.jopari_claim_alerts where
        pm_sk = 2097429
        and src_sk = 3081227
        and pat_acct = '21800168745'
        and note = 'P3:95 - pending for additional information not received'
        and code = '277-P3:95');
update
      business_analysis.jopari_claim_alerts a
set  
      cust_id = b.cust_id
      , claim_num = b.claim_num
      , charges = b.charges
      , patient_name = b.patient_name
      , policy_number = b.policy_number
      , insurance_name = b.insurance_name
      , effective_date = '20220121'::date
      , assigned = 'Clearinghouse BA'
      , processed = False
from 
      tpl_billing_records b
where 
      b.pm_sk  = a.pm_sk
      and a.created_at::date = now()::date
      and a.pm_sk = 2097429
      and a.processed is null;

