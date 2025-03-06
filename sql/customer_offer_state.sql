
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


DROP TABLE IF EXISTS "customer_offer_state";

CREATE TABLE "customer_offer_state"(
    "id" UUID NOT NULL DEFAULT uuid_generate_v4(),
    "queue_job_id"  UUID NOT NULL,
    "queue_job_created_ts" TIMESTAMP(3) NOT NULL,
    "created_ts" TIMESTAMP(3) NOT NULL DEFAULT NOW(),
    "cnum" VARCHAR(255) NOT NULL,
    "camp_code" VARCHAR(255) NOT NULL,
    "camp_action" VARCHAR(255) NOT NULL
);

truncate table customer_offer_state;

ALTER TABLE "customer_offer_state" ADD PRIMARY KEY("id");

CREATE UNIQUE INDEX uk_customer_offer_state ON customer_offer_state  ("cnum", "queue_job_id", "camp_code")

CREATE INDEX idx_customer_offer_state_created_ts ON customer_offer_state (created_ts);


select * from customer_offer_state;


select count(*) from customer_offer_state;


select * from customer_offer_state




DROP TABLE IF EXISTS "offer_action";

CREATE TABLE "offer_action"(
    "id" UUID NOT NULL,
    "created_ts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "cnum" VARCHAR(255) NOT NULL,
    "camp_code" VARCHAR(255) NOT NULL,
    "action_type" VARCHAR(255) NOT NULL,
    "camp_action" VARCHAR(255) NOT NULL
);

truncate table offer_action;

ALTER TABLE "offer_action" ADD PRIMARY KEY("id");


select count(*) from offer_action 


select * from offer_action order by created_ts desc;


with prev_job_id as (
	select 
		queue_job_id 
	from customer_offer_state
	where cnum = 'LW7McK2' 
	and created_ts < (
		select created_ts from customer_offer_state
		where queue_job_id = '245bd27c-f041-4d70-82ae-ca4018c53d00'
		limit 1
	)
	order by created_ts desc 
	limit 1
)
, prev_state as (
	select * 
	from customer_offer_state
	where queue_job_id = (select queue_job_id from prev_job_id)
)
, curr_state as (
	select * from customer_offer_state 
	where queue_job_id = '245bd27c-f041-4d70-82ae-ca4018c53d00'
)
, full_joined as (
	select 
		cs.cnum as c_cnum
		, cs.camp_code as c_camp_code
		, '|'
		, ps.cnum as p_cnum
		, ps.camp_code as p_camp_code
	from curr_state cs 
	full join prev_state ps 
		on cs.camp_code = ps.camp_code
)
, act as (
	select 
		coalesce(c_cnum, p_cnum) as cnum
		, coalesce(c_camp_code, p_camp_code) as camp_code
		, case 
			when p_cnum is null then 'enable' 
			when c_cnum is null then 'disable' 
			else 'do_nothing' 
		end as action_type
	from full_joined fj
	where c_cnum is null or p_cnum is null
)
insert into offer_action (
    id,
    created_ts,
    cnum,
    camp_code,
    action_type,
    camp_action
)
select 
    uuid_generate_v4() as id
    , now() as created_ts
	, cnum
	, camp_code
	, action_type
	, lower(camp_code || '__' || action_type) as camp_action
from act











select cnum, count(*) as cnt
from (
	select cnum, queue_job_id
	from customer_offer_state
	group by cnum, queue_job_id
	having count(*) > 1
)
group by cnum
having count(*) > 1
order by cnt desc

select --distinct queue_job_id 
	*
from customer_offer_state where cnum = 'LW7McK2';

delete from customer_offer_state
where id in (
	'c61db739-8852-42b7-b0b1-5db649266665'::uuid
	, '4d55d445-c378-4b21-b609-a344857c4969':: uuid
	, '91c0325d-f4a7-448b-86c9-f5d3a5eac660'::uuid
	, '2127f0a0-a3ad-4670-80fd-9d0b944e571f'::uuid
	, '839d663e-2912-44fc-b7e4-98c5622c90f8'::uuid
	, 'b07a212a-8574-46db-8198-577c56557554'::uuid
)




select 
            id
            , created_ts::text
            , event_id
            , cnum
            , camp_code
            , action_type
            , start_ts::text
            , end_ts::text
     from offer_nomination



