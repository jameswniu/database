import os
import sys
import re

import psycopg2.extras

from config import user_db, passwd_db


os.chdir(r'L:\Auto_Opportunity_Analysis\Enrollment')
file = '483_THR_Yi_20210511.txt'        ## SPECIFY filename
custid = re.search('\d{3}', file).group()


#----
# extract fields
#----
dicy = {}
cnt = 0
with open(file) as fw:
    for line in fw:
        taxid = line.split('|')[0].strip().replace('-', '')
        provider = line.split('|')[1].strip().replace(',', '').replace("'", "").replace('-', '')
        dicy[taxid] = provider

        cnt += 1
print(cnt)


#----
# insert record one-by-one
#----
params = {
    'host': 'revpgdb01.revintel.net',
    'database': 'tpliq_tracker_db',
    'user': user_db,
    'password': passwd_db
}
con = psycopg2.connect(**params)
for i in dicy:
    sql = """\
insert into
	business_analysis.enrollment_tracking (cust_id, billing_provider_taxid, billing_provider)
values
	({}, '{}', '{}');""".format(custid, i, dicy[i])
    with con:
        cur = con.cursor()
        cur.execute(sql)
        print(sql)


#----
# update cust name
#----
sql1 = """\
update
	business_analysis.enrollment_tracking a
set
	cust_name = b.cust_name
from
    tpl_cust_infos b
where
	a.cust_id = b.cust_id;"""
with con:
    cur = con.cursor()
    cur.execute(sql1)
    print(sql1)


#----
# update npi
#----
sql2 = """\
with cte0 as (
select 
    max(cust_id) cust_id
    , billing_provider_taxid
    , string_agg(distinct billing_provider, ';  ') billing_provider
    , string_agg(distinct billing_provider_npi, '; ') billing_provider_npi
    , string_agg(distinct billing_provider_addr, ';   ') billing_provider_addr
    , string_agg(distinct billing_provider_phone, '; ') billing_provider_phone
from (
    select distinct 
    cust_id
    , content->>'billing_provider_taxid' billing_provider_taxid
    , content->>'billing_provider' billing_provider
    , content->>'billing_provider_npi' billing_provider_npi
    , concat(content->>'billing_provider_addr1', ', ', content->>'billing_provider_city', ', ', content->>'billing_provider_state', ' ', content->>'billing_provider_zip') billing_provider_addr
    , nullif(content->>'billing_provider_phone', '') billing_provider_phone
from 
    tpl_pre_billing_records a
where 
    cust_id = {}
    and exists (select 1 from business_analysis.enrollment_tracking b where a.content->>'billing_provider_taxid' = b.billing_provider_taxid)
    ) foo0
group by 
    billing_provider_taxid)""".format(custid)
sql3 = """\
update business_analysis.enrollment_tracking a
set 
	billing_provider_npi = b.billing_provider_npi
from 
    cte0 b
where 
    a.cust_id = {}
    and a.billing_provider_taxid = b.billing_provider_taxid
    and a.billing_provider_npi is null;""".format(custid)
sql3a = sql2 + '\n' + sql3
with con:
    cur = con.cursor()
    cur.execute(sql3a)
    print(sql3a)


#----
# update addr
#----
sql4 = """\
update business_analysis.enrollment_tracking a
set 
	billing_provider_addr = b.billing_provider_addr
from 
    cte0 b
where 
    a.cust_id = {}
    and a.billing_provider_taxid = b.billing_provider_taxid
    and a.billing_provider_addr is null;""".format(custid)
sql4a = sql2 + '\n' + sql4
with con:
    cur = con.cursor()
    cur.execute(sql4a)
    print(sql4a)


#----
# update phone
#----
sql5 = """\
update business_analysis.enrollment_tracking a
set 
	billing_provider_phone = b.billing_provider_phone
from 
    cte0 b
where 
    a.cust_id = {}
    and a.billing_provider_taxid = b.billing_provider_taxid
    and a.billing_provider_phone is null;""".format(custid)
sql5a = sql2 + '\n' + sql5
with con:
    cur = con.cursor()
    cur.execute(sql5a)
    print(sql5a)


print(f'{cnt} records added and completed')

