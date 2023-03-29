-- drop all views / tables
if object_id('v_votes_by_phone_number') is not null drop view v_votes_by_phone_number;
if object_id('v_votes_by_contestant_number_state') is not null drop view v_votes_by_contestant_number_state;
if object_id('area_code_state') is not null drop table area_code_state;
if object_id('votes') is not null drop table votes;
if object_id('contestants') is not null drop table contestants;

-- contestants table holds the contestants numbers (for voting) and names
create table contestants
(
  contestant_number integer     not null
, contestant_name   varchar(50) not null
, primary key
  (
    contestant_number
  )
);

-- map of area codes and states for geolocation classification of incoming calls
create table area_code_state
(
  area_code smallint   not null
, state     varchar(2) not null
, primary key
  (
    area_code
  )
);

-- votes table holds every valid vote.
--   voters are not allowed to submit more than <x> votes, x is passed to client application
create table votes
(
  vote_id            bigint     not null
, phone_number       bigint     not null
, state              varchar(2) not null
, contestant_number  integer    not null references contestants (contestant_number)
, created            timestamp  not null
);
create index idx_votes_phone_number on votes (phone_number);

-- rollup of votes by phone number, used to reject excessive voting
create view v_votes_by_phone_number
(
  phone_number
, num_votes
)
as
   select phone_number
        , count(*)
     from votes
 group by phone_number
;

-- rollup of votes by contestant and state for the heat map and results
create view v_votes_by_contestant_number_state
(
  contestant_number
, state
, num_votes
)
as
   select contestant_number
        , state
        , count(*)
     from votes
 group by contestant_number
        , state
;
