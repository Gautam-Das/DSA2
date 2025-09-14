stuff that still needs to be done:
- implement retries when sending requests to the server
- update the write function to also write:
  - the time when it was updated for the 30 seconds
  - the update number for the last 20 updates
  - the port from which it is currently connected, so that if connection closes we dont return the update from that one
- don't close connection after responding to both client (??) and content
- write your own json parser
- clean code with linting