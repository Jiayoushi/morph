## Morph: A Simple Distribtued Filesystem

Morph, a toy distributed filesystem, is consisted of three components, 
Metadata Server, Monitor and Object Store. Clients uses metadata 
server to manipulate files metadata (eg., mkdir, open), and use consistent
hashing to obtain the object store to do data reads and writes. Monitor
uses Paxos to ensure the consistent view of the of the object store cluster
 map, which is used for consistent hasing. Object store provides the
persistent storage for files' metadata and data.

See the docs folder for more information about its design.
