Full name:  Team_T_05

Which features did you implement?
  Entire Project

  Here is a breakdown of what was built:

  KV Storage Layer
  - Custom key-value store with (row, column) -> value data model
  - PUT, GET, CPUT (compare-and-swap), and DELETE operations
  - Write-ahead log (WAL) for crash durability
  - Checkpoint + WAL replay for fast recovery on restart
  - Bloom filter to skip disk reads for missing keys
  - Per-row shared_mutex locking for concurrent access

  Replication and Fault Tolerance
  - Primary-backup replication with synchronous write forwarding to secondaries
  - LSN (Log Sequence Number) tracking on every write
  - Secondary gap detection: if a replica misses writes it triggers a resync
  - Full snapshot + WAL delta resync so a restarted node can rejoin in seconds
  - Coordinator-driven failover: picks the secondary with the highest LSN as new primary

  Coordinator
  - Heartbeat-based failure detection (500ms interval, 3 misses = node dead)
  - LOOKUP protocol: frontend asks coordinator which node is primary for a given row key
  - Leader election on primary failure (highest-LSN secondary is promoted)
  - Automatic recovery: re-syncs a rejoining node and adds it back as a secondary
  - Tablet-based sharding: keyspace divided into row-key ranges, each range served by its own replica group

  Frontend HTTP Servers
  - Three stateless HTTP server instances (fe1, fe2, fe3) on ports 8090/8091/8092
  - Custom HTTP/1.1 parser (no external library), supports chunked transfer encoding
  - 32-thread pool per server, 64 MB request body limit
  - Cookie-based sessions stored in the KV layer (frontend holds no state)
  - Authentication: login, signup, logout, change password

  Email (Webmail + SMTP)
  - Inbox, sent, and trash views
  - Compose and send email (to local users and external SMTP)
  - Move to trash and restore from trash
  - File attachments on emails
  - Contacts list (add/delete)
  - Inbound SMTP server (port 2525) accepts mail from external senders
  - SSE (Server-Sent Events) push so inbox updates in the browser without polling

  Drive (File Storage)
  - Upload and download files
  - Create folders, rename, move, and delete files and folders
  - Storage quota per user with quota status and update endpoints

  Chat
  - Group chat rooms with message history
  - Direct messages (DMs) between users
  - All messages persisted in the KV store

  Admin Console
  - Dashboard showing live cluster status (which nodes are up/down, which is primary)
  - Kill and restart individual KV nodes from the browser for demo/testing

Did you complete any extra-credit tasks? If so, which ones?
  - Bloom filter on the KV storage layer (avoids disk reads for keys that do not exist)
  - Write coalescing on the replication path (batches replicated writes to reduce round-trips)
  - Multi-tablet sharding across multiple replica groups (tablet_aa_am, tablet_an_zz, etc.)
  - Multiple stateless frontend servers with automatic failover

Did you personally write all the code you are submitting
(other than code from the course web page)?
  [x] Yes

Did you copy any code from the Internet, or from classmates?
  [ ] Yes
  [x] No

Did you collaborate with anyone on this project?
  [x] Yes
  [ ] No

  This was a four-person team project (Team T05):
  - Nikunj  (nikunj@seas.upenn.edu)  -- KV store, WAL, Bloom filter
  - Rohit   (rohit57@seas.upenn.edu) -- Coordinator, replication, recovery
  - Liudawei (liudawei@seas.upenn.edu) -- Frontend HTTP server, webmail, SSE
  - Yke     (yke@seas.upenn.edu)     -- Drive, admin console

Did you use any AI tool such as ChatGPT for this assignment?
  [] Yes
  [X] No

How many hours did you spend on this assignment?
  Approximately 320 hours total across the team (roughly 80 hours per person).
