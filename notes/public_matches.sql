-- opendota
CREATE TABLE IF NOT EXISTS public_matches (
  match_id bigint PRIMARY KEY,
  match_seq_num bigint,
  radiant_win boolean,
  start_time integer,
  duration integer,
  lobby_type integer,
  game_mode integer,
  avg_rank_tier double precision,
  num_rank_tier integer,
  cluster integer,
  radiant_team integer[],
  dire_team integer[]
);