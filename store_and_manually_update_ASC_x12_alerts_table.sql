--------------------------------------------------------------------------------------------------------------------------
-- summarize alerts error
--------------------------------------------------------------------------------------------------------------------------
/*status james claims*/
select 
	effective_date
	, processed
	, count(*) bills
	, string_agg(distinct note, ';   ') jopari_error
from 
	business_analysis.jopari_claim_alerts
where 
	assigned ~* 'James'
--	and processed = 'f'
group by
	processed, effective_date
order by 
	effective_date desc;
/*status others claims*/
select 
	effective_date
	, processed
	, count(*) bills
	, string_agg(distinct note, ';   ') jopari_error
from 
	business_analysis.jopari_claim_alerts
where 
	assigned !~* 'James'
--	and processed = 'f'
group by 
	processed, effective_date
order by 
	effective_date desc;


--------------------------------------------------------------------------------------------------------------------------
-- query alerts error
--------------------------------------------------------------------------------------------------------------------------
/*query*/
select 
	* 
from 
	business_analysis.jopari_claim_alerts
where
--	created_at::date = current_date
	pm_sk in (2475068
	)
--	pat_acct in ('13930765', '7448742', '7586207')
--	and processed = 'f'
--	insurance_name ~* 'progressive'
--	and assigned ~* 'james'
--	code = '277-A6:189'
--	processed = True
--	processed_at is not null
order by 
	effective_date desc;

delete
from 
	business_analysis.jopari_claim_alerts
where
	created_at::date = current_date;


--------------------------------------------------------------------------------------------------------------------------
-- update manually alerts error
--------------------------------------------------------------------------------------------------------------------------
/*already billed no issues*/
with cte0 as (
select max(cust_id) cust_id, max(pat_acct) pat_acct, pm_sk, max(created_at)::date bill_date 
from tpl_billing_records group by pm_sk
)
-->
update
	business_analysis.jopari_claim_alerts a
set
	processed = True
	, processed_at = b.bill_date
from
	cte0 b
where 
	a.pm_sk = b.pm_sk and a.pm_sk in (2379786, 2379801, 2382780
)
	and processed = False;
/*change actor*/
update 
	business_analysis.jopari_claim_alerts 
set
	assigned = 'Clearinghouse BA'
where 
	pm_sk = [x]
and 
	assigned = 'MLX Yi James';
update 
	business_analysis.jopari_claim_alerts 
set
	assigned = 'Payer followup MGT'
	, note = 'A7:97:PR - Patient eligibility not found with the payer'
	, code = '277-A7:97:PR'
where 
	pm_sk = 2314798
and 
	assigned = 'MLX Yi James';
--update 
--	business_analysis.jopari_claim_alerts 
--set
--	assigned = 'MLX Yi James'
--	, note = 'A6:189 - missing admission date  Required for inpatient'
--	, code = '277-A6:189'
--where
--	pm_sk = 2213804
--and
--	assigned = 'Payer followup MGT';


--------------------------------------------------------------------------------------------------------------------------
-- resend fax (non-WC) or edi WC
--------------------------------------------------------------------------------------------------------------------------
/*check billing records if already sent*/
with cte0 as (
select
	a.bill_sent_at
	, b.content->>'vx_carrier_lob' lob
	, a.pm_sk
	, a.sending_method
	, a.fax
	, a.accident_state
	, a.cust_id 
	, a.pat_acct 
	, a.claim_num
	, a.charges 
	, a.insurance_name
from 
	tpl_billing_records a
left join 
	tpl_pre_billing_records b
	on a.pm_sk = b.pm_sk
where
	a.src_sk in (3401780
	)
--	a.pat_acct in (
--	'298322417/11', 
--	'11241856'
--	)
--and 
--	a.claim_num in (
--	'ITU9956'
--	)
--a.pm_sk in (2464942
--, 2464975
--, 2472534
--, 2475068
--)
order by
	pat_acct
	, bill_sent_at desc)
select * from cte0;
select pm_sk, cust_id, pat_acct, content->>'vx_carrier_lob' vx_carrier_lob, content->>'vx_accident_state' vx_accident_state, content->>'vx_carrier_insured_state' vx_carrier_insured_state,
content->>'service_date' service_date from tpl_pre_billing_records 
where pm_sk in (select pm_sk from cte0);
/*gen table*/
drop table if exists test_responses;
create table if not exists test_responses as (
select
	*
from 
	tpl_pre_billing_records 
where
--	pat_acct in ('21000876462'
--	)
	pm_sk in (2418411
)
);
-->
select
	date(created_at) created_prebilling
	, pm_sk
	, content->>'vx_carrier_lob' vx_carrier_lob
	, content->>'claim_type' inst_prof
	, insurance_name, content->>'vx_carrier_insured_state' vx_carrier_insured_state
	, content->>'vx_claim_type' fp_tp
	, content->>'send_fax_number' send_fax_number
	, content->>'edi_payer_id' edi_payer_id
	, cust_id, pat_acct, claim_num
	, concat(content->>'vx_carrier_insured_first_name', '|', content->>'vx_carrier_patient_first_name', '|', content->>'patient_firstname') first_name
	, concat(content->>'vx_carrier_insured_last_name', '|', content->>'vx_carrier_patient_last_name', '|', content->>'patient_lastname') last_name
	, concat(content->>'vx_carrier_insured_address_1', '|', content->>'vx_carrier_patient_address_1', '|', content->>'patient_addr1') addr1
	, concat(content->>'vx_carrier_insured_pone', '|', content->>'vx_carrier_patient_phone', '|', content->>'patient_phone') phone
	, content->>'vx_accident_state' vx_accident_state
	, content->>'vx_date_of_loss' vx_date_of_loss
	, content->>'admission_date' admission_date
	, content->>'discharge_date' discharge_date
	, content->>'facility_code' facility_code
	, content->>'claim_filing_indicator' claim_filing_indicator, content
from 
	business_analysis.test_responses
order by
	insurance_name;


/*update edi WC resend*/
update
	business_analysis.test_responses
set
	content = content || '{"edi_payer_id": "19046"}'
where 
	content->>'claim_type' = '837P'
	and content->>'vx_carrier_lob' = 'WORKERS COMP & EMPLOYERS LIABILITY'
	and insurance_name = 'TRAVELERS INDEMNITY COMPANY';
update
	business_analysis.test_responses
set
	content = content || '{"edi_payer_id": "J1554"}'
where 
	content->>'claim_type' = '837P'
	and content->>'vx_carrier_lob' = 'WORKERS COMP & EMPLOYERS LIABILITY'
	and insurance_name = 'CHUBB INSURANCE COMPANY OF NEW JERSEY';
update
	business_analysis.test_responses
set
	content = content || '{"edi_payer_id": "J3349"}'
where
	content->>'claim_type' = '837P'
	and content->>'vx_carrier_lob' = 'WORKERS COMP & EMPLOYERS LIABILITY'
	and insurance_name = 'STATE FARM (R) AFFILIATE';
update
	business_analysis.test_responses
set
	content = content || '{"edi_payer_id": "33600"}'
where
	content->>'claim_type' = '837P'
	and content->>'vx_carrier_lob' = 'WORKERS COMP & EMPLOYERS LIABILITY'
	and insurance_name ~* 'LIBERTY MUTUAL';


/*update fax (non WC) resend*/
with cte0 as (select 'PROG'::text holder) -- SPECIFY
-->
/*find fax number frm carrier infos table*/
select * from tpl_carrier_infos a where exists (select 1 from cte0 b where a.insurance_name ~* b.holder) order by insurance_name;
/*find fax number from past billed*/
select
	max(a.created_at) last_billed
	, format('%s-%s-%s'
		, left(case when length(replace(fax, '-', '')) > 10 then right(replace(fax, '-', ''), 10) else replace(fax, '-', '') end, 3)
		, substring(case when length(replace(fax, '-', '')) > 10 then right(replace(fax, '-', ''), 10) else replace(fax, '-', '') end, 4, 3)
		, right(case when length(replace(fax, '-', '')) > 10 then right(replace(fax, '-', ''), 10) else replace(fax, '-', '') end, 4)
		) fax
	, a.insurance_name, a.claim_type
--	, string_agg(distinct c.content->>'vx_carrier_insured_state', ';  ') vx_carrier_insured_state
	, count(*)
from 
	tpl_billing_records a
--left join
--	tpl_pre_billing_records c on a.pm_sk = c.pm_sk
where 
	exists (select 1 from cte0 b where a.insurance_name ~* b.holder)
	and nullif(fax, '') is not null
group by
	case when length(replace(fax, '-', '')) > 10 then right(replace(fax, '-', ''), 10) else replace(fax, '-', '') end, a.insurance_name, a.claim_type
order by 
	max(a.created_at) desc
	, count(*) desc
limit 20;


update
	business_analysis.test_responses a
set 
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "833-958-1066"}'
where
	pm_sk = 2418411;


/*update geico fax*/
update 
	business_analysis.test_responses a
set 
	content = content #- '{"edi_payer_id"}' || jsonb(format('{"send_fax_number": "%s"}', b.fax))
from 
	business_analysis.geico_fax b
where 
	a.insurance_name ~* 'GEICO|GOVERNM' and a.content->>'vx_carrier_insured_state' = b.state;
/*update others*/
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "866-447-4293"}'
where insurance_name ~* 'ALLSTATE';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "888-268-8840"}'
where insurance_name ~* 'LIBERTY MUTUAL';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "866-268-8494"}'
where insurance_name ~* 'MERCURY GENERAL';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "877-213-7258"}'
where insurance_name ~* 'PROGRESSIVE';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "855-820-6318"}'
where insurance_name ~* 'STATE FARM' and content->>'vx_claim_type' = 'TP';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "844-218-1140"}'
where insurance_name ~* 'STATE FARM' and content->>'vx_claim_type' = 'FP';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "866-828-2330"}'
where insurance_name ~* 'USAA' and content->>'vx_claim_type' = 'TP';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "888-272-1255"}'
where insurance_name ~* 'USAA' and content->>'vx_claim_type' = 'FP';
update 
	business_analysis.test_responses a
set
	content = content #- '{"edi_payer_id"}' || '{"send_fax_number": "888-307-3180"}'
where insurance_name ~* 'GREAT AMERICAN';


grant select, update, insert, delete on
    business_analysis.analyst_shadow_bills
to
    melvin_andoor_u;


select
    created_at::date
    , cust_id
    , count(*)
from
    business_analysis.analyst_shadow_bills
where
    cust_id in (631, 538, 734)
group by
    created_at::date, cust_id
order by
    created_at::date desc
    , cust_id
limit 10;


--------------------------------------------------------------------------------------------------------------------------
-- miscellaneous
--------------------------------------------------------------------------------------------------------------------------
/*geico_fax table*/
--drop table if exists business_analysis.geico_fax;
create table if not exists business_analysis.geico_fax as (
select 
	created_at, state, format('%s-%s-%s', left(fax, 3), substring(fax, 4, 3), right(fax, 4)) fax 
from (values
	(timezone('utc', now()), 'AL', '2023544691'),
	(timezone('utc', now()), 'AK', '8667602860'),
	(timezone('utc', now()), 'AZ', '8665682132'),
	(timezone('utc', now()), 'AR', '2023544691'),
	(timezone('utc', now()), 'CA', '6198194004'),
	(timezone('utc', now()), 'CO', '2144425164'),
	(timezone('utc', now()), 'CT', '7168980542'),
	(timezone('utc', now()), 'DE', '7037382188'),
	(timezone('utc', now()), 'DC', '7037382188'),
	(timezone('utc', now()), 'FL', '2023545295'),
	(timezone('utc', now()), 'GA', '2023544691'),
	(timezone('utc', now()), 'HI', '8667602860'),
	(timezone('utc', now()), 'ID', '8665682132'),
	(timezone('utc', now()), 'IL', '2023544691'),
	(timezone('utc', now()), 'IN', '2023544691'),
	(timezone('utc', now()), 'IA', '2144425164'),
	(timezone('utc', now()), 'KS', '2144425164'),
	(timezone('utc', now()), 'KY', '2023544691'),
	(timezone('utc', now()), 'LA', '2023544691'),
	(timezone('utc', now()), 'ME', '7168980542'),
	(timezone('utc', now()), 'MD', '7037382188'),
	(timezone('utc', now()), 'MA', '7168980542'),
	(timezone('utc', now()), 'MI', '2023544691'),
	(timezone('utc', now()), 'MN', '2023544691'),
	(timezone('utc', now()), 'MS', '2023544691'),
	(timezone('utc', now()), 'MO', '2023544691'),
	(timezone('utc', now()), 'MT', '8665682132'),
	(timezone('utc', now()), 'NE', '2144425164'),
	(timezone('utc', now()), 'NV', '8665682132'),
	(timezone('utc', now()), 'NH', '7168980542'),
	(timezone('utc', now()), 'NJ', '7168980542'),
	(timezone('utc', now()), 'NM', '2144425164'),
	(timezone('utc', now()), 'NY', '7168980542'),
	(timezone('utc', now()), 'NC', '7037382194'),
	(timezone('utc', now()), 'ND', '2144425164'),
	(timezone('utc', now()), 'OH', '2023544691'),
	(timezone('utc', now()), 'OK', '2144425164'),
	(timezone('utc', now()), 'OR', '8665682132'),
	(timezone('utc', now()), 'PA', '7037382188'),
	(timezone('utc', now()), 'RI', '7168980542'),
	(timezone('utc', now()), 'SC', '2023544691'),
	(timezone('utc', now()), 'SD', '2144425164'),
	(timezone('utc', now()), 'TN', '2023544691'),
	(timezone('utc', now()), 'TX', '2144425164'),
	(timezone('utc', now()), 'UT', '8665682132'),
	(timezone('utc', now()), 'VT', '7168980542'),
	(timezone('utc', now()), 'VA', '7037382194'),
	(timezone('utc', now()), 'WA', '8665682132'),
	(timezone('utc', now()), 'WV', '7037382188'),
	(timezone('utc', now()), 'WI', '2023544691'),
	(timezone('utc', now()), 'WY', '2144425164')
) foo0(created_at, state, fax)
);
--alter table business_analysis.geico_fax add unique(state);
);
-->
select * from geico_fax;


/*add or modify geico fax*/
insert into 
	business_analysis.geico_fax(created_at, state, fax)
values
	(timezone('utc', now()), 'AL', '202-354-4691')
	, (timezone('utc', now()), 'AK', '866-760-2860')
on conflict(state) 
	do update set created_at = excluded.created_at, fax = excluded.fax


/*initialize table*/
--drop table if exists business_analysis.jopari_claim_alerts;
create table if not exists business_analysis.jopari_claim_alerts as (
	select
	created_at
	, pm_sk
	, cust_id
	, pat_acct
	, claim_num
	, src_sk
	, null::text claim_type
	, charges
	, patient_name
	, policy_number
	, insurance_name
	, null::text vx_carrier_lob
	, null::text work_comp_flag
	, note
	, code
	, effective_date
	, processed
	, null::timestamp processed_at
	, null::text assigned
from 
	tpl_claim_responses limit 0
);
select * from business_analysis.jopari_claim_alerts;


/*add columns*/
drop table if exists jopari_claim_resps;
alter table business_analysis.jopari_claim_alerts add column vx_carrier_lob text;
alter table business_analysis.jopari_claim_alerts add column work_comp_flag text;
alter table business_analysis.jopari_claim_alerts rename to jopari_claim_resps;
create table if not exists business_analysis.jopari_claim_alerts as (
select
created_at
, cust_id
, pat_acct
, claim_num
, pm_sk
, src_sk
, charges
, claim_type
, patient_name
, policy_number
, insurance_name
, vx_carrier_lob
, work_comp_flag
, note
, code
, effective_date
, processed
, processed_at
, assigned
from jopari_claim_resps
);
update business_analysis.jopari_claim_alerts a set vx_carrier_lob = b.content->>'vx_carrier_lob' from tpl_pre_billing_records b where a.pm_sk = b.pm_sk and a.vx_carrier_lob is null;
update business_analysis.jopari_claim_alerts a set work_comp_flag = nullif(b.content->>'work_comp_flag', '') from tpl_pre_billing_records b where a.pm_sk = b.pm_sk and a.work_comp_flag is null;


/*restore (processed or actor) from backup*/
drop table if exists business_analysis.jopari_claim_alerts;
create table if not exists business_analysis.jopari_claim_alerts as (
select * from jopari_claim_alerts_backup
)


/*select and check Work Comp claims*/
select
    --bill_sent_at
    --, sending_method
    --, fax
    --, accident_state
    cust_id
    , pat_acct
    , claim_num
    , insurance_name
    , pm_sk
    , src_sk
    , content->>'vx_accident_state' vx_accident_state
    , content->>'vx_carrier_insured_state' vx_carrier_insured_state
    , content->>'edi_payer_id' edi_payer_id
    , content->>'vx_carrier_lob' vx_carrier_lob
    , content->>'work_comp_flag' work_comp_flag
from
    tpl_pre_billing_records
where 
	pat_acct in (
    '13330318'
    , '21800223171'
    , '6671519'
    , '13905656'
    , '7011290'
    , '7011290'
    )
and claim_num in (
    '4A2109GF29A0001'
    , 'A00314788'
    , '076921019514'
    , '040521036320'
    , '4622R764P'
    , '4A21075C5440001'
    )
;


/*Work Comp billing contact*/
	with cte0 as (
	select
		foo0.cust_id
		, foo0.pat_acct
		, foo3.content->>'claim_type' prof_inst
		, foo3.content->>'vx_claim_type' fp_tp
		, foo3.content->>'vx_carrier_lob' carrier_lob
		, foo0.insurance_name
		, foo3.content->>'edi_payer_id' edi_payer_id
		, foo0.sending_method
		, case when foo0.sending_method = 'edi' then null else 
			format('%s-%s-%s'
			, left(case when length(replace(foo0.fax, '-', '')) > 10 then right(replace(foo0.fax, '-', ''), 10) else replace(foo0.fax, '-', '') end, 3)
			, substring(case when length(replace(foo0.fax, '-', '')) > 10 then right(replace(foo0.fax, '-', ''), 10) else replace(foo0.fax, '-', '') end, 4, 3)
			, right(case when length(replace(foo0.fax, '-', '')) > 10 then right(replace(foo0.fax, '-', ''), 10) else replace(foo0.fax, '-', '') end, 4)
			)
		end fax
		, foo0.charges billed_amt
		, foo0.bill_sent_at
		, nullif(foo1.charges, 0) trans_amt
		, foo1.trans_date
		, foo2.invoiced_at::date
	from (
		select * from tpl_billing_records
	) foo0
	inner join (
		select * from tpl_mva_trans
		where duplicate_payment = false
	) foo1 on foo0.cust_id = foo1.cust_id and foo0.pat_acct = foo1.pat_acct
	left join (
		select * from tpl_mva_invoice_details
		where duplicate_payment = false
	) foo2 on foo0.cust_id = foo2.cust_id and foo0.pat_acct = foo2.pat_acct
	left join (
		select * from tpl_pre_billing_records
	) foo3 on foo0.cust_id = foo3.cust_id and foo0.pat_acct = foo3.pat_acct
	where
--		foo0.bill_sent_at::text ~* '2021'
		foo3.content->>'vx_carrier_lob' != 'AUTO (PERSONAL)'
		and foo3.content->>'vx_carrier_lob' != 'AUTO (COMMERCIAL)'
		and nullif(foo1.charges, 0) is not null
	--	and foo0.cust_id = 734
--	order by 
--		foo0.bill_sent_at desc
	)
-->
select 
	carrier_lob
	, fp_tp
	, insurance_name
	, sending_method
	, edi_payer_id
	, fax
	, string_agg(distinct bill_sent_at::text, ';  ') bill_sent_at
	, string_agg(distinct trans_date::text, ';  ') trans_date
	, string_agg(distinct invoiced_at::text, ';  ') invoiced_at
	, string_agg(distinct format('%s-%s', cust_id, pat_acct), ';  ') cust_idNpat_acct
from 
	cte0
where
	not (edi_payer_id is null and fax is null)
	and invoiced_at is not null
group by
	carrier_lob
	, fp_tp
	, insurance_name
	, sending_method
	, edi_payer_id
	, fax
order by
	insurance_name
;


/*check individual claims billed if Work Comp*/
select
	foo0.lob
	, foo1.billed_at
	, foo0.cust_id
	, foo0.pat_acct
	, foo0.pm_sk
	, foo0.fax
	, foo0.insurance_name
	, foo0.tp_fp
from (
	select
		cust_id,
		pat_acct,
		pm_sk,
		nullif(content->>'send_fax_number', '') fax,
		insurance_name,
		content->>'vx_claim_type' tp_fp,
		content->>'vx_carrier_lob' lob
	from
		tpl_pre_billing_records
	where
		nullif(content->>'send_fax_number', '') is not null
	) foo0
right join (
	select
		created_at::date billed_at,
		pm_sk
	from
		tpl_billing_records
	where
		sending_method = 'fax'
	) foo1
	on foo0.pm_sk = foo1.pm_sk
where
	foo0.insurance_name ~* 'ALLSTATE'

