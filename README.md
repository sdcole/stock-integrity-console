# stock-integrity-console
App that monitors the database for bad data.
Scheduled to run weekly

## Change Log
- 1.0.1 Search Range Update and Other Bugfixes
    - Modified App to go back 4 years.
    - Improved the thread safety (No longer passes the search dates by reference to share across threads).
