CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

CREATE USER IF NOT EXISTS user_full IDENTIFIED WITH plaintext_password BY 'full_pass';
CREATE USER IF NOT EXISTS user_limited IDENTIFIED WITH plaintext_password BY 'limited_pass';

GRANT analyst_full TO user_full;
GRANT analyst_limited TO user_limited;

GRANT SELECT ON dataeng.* TO user_full;
GRANT SELECT ON dataeng.vw_trip_summary_limited TO user_limited;