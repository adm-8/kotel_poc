DROP TABLE IF EXISTS "offer_nomination";

CREATE TABLE "offer_nomination"(
    "id" UUID NOT NULL,
    "created_ts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "event_id" UUID NOT NULL,
    "cnum" VARCHAR(255) NOT NULL,
    "camp_code" VARCHAR(255) NOT NULL,
    "action_type" SMALLINT NOT NULL,
    "start_ts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL,
    "end_ts" TIMESTAMP(0) WITHOUT TIME ZONE NOT NULL
);

ALTER TABLE "offer_nomination" ADD PRIMARY KEY("id");

	
CREATE INDEX idx_offer_nomination_cnum ON offer_nomination (cnum);
CREATE INDEX idx_offer_nomination_start_ts ON offer_nomination (start_ts);
CREATE INDEX idx_offer_nomination_end_ts ON offer_nomination (end_ts);





SELECT DISTINCT cnum FROM offer_nomination


select * from public.offer_nomination



truncate table offer_nomination;

select count(*) from offer_nomination;

select * from offer_nomination;



select cnum , count(*) from offer_nomination
group by cnum


select cnum from offer_nomination


select 
	camp_code
	, case when min(action_type) = 0 then 'deactivate' else 'activate' end as camp_action
from offer_nomination
where 
	cnum = 'oB1Rah3'
	and now() between start_ts and end_ts -- 0.987
group by 
	camp_code



CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
