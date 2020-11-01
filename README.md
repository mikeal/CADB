# CADB

Experimental single file content addressed database.

This is a single file database for storing content addressed block data. You can think of it
like a key/value store where the keys **MUST** be hash digests of a sufficient length.

CADB is a special B+ tree which is both sorted and balanced by the hash digest. The keys (digests) are
sorted by binary comparison and the tree's structure is chunked using randomness derived from the tail
of each hash. This produced a self-balancing and well sorted structure on-disc.

The benefits of this approach are:

* Predictable performance over time.
  * As the entries in the store increase the tree depth logorithmically scales to the new size.
  * The store never needs to be compacted in order to improve performance since the tree is
    deterministically balanced. Compaction only serves to remove orphaned data, read/write performance
    remains the same.
  * The depth of the tree can be used to estimate the size of the branch data in the top of the tree.
    This means branch reads can simply be stored in an LRU. This means there's no need to manually eject
    orphaned data or memmap the database file, the cache can predictably adjust to the needs of the tree.
* Portability
  * The database file can be copied at any time, sent around, and a new store can use it immediately with
    zero load time.
  * This combines a lot of the workflow we use CAR files for with our block storage needs into a tidy single
    package.
  * Since the tree is deterministally structured, a compacted CADB file can be consistently hashed.
  * Since the database is guaranteed to be a single map keyed by digest, the hash of a CADB file can be used in a CID
    and we can walk the node structure in IPLD.
    
## CADB Page File

Every write to disc **MUST** be a single page file. This means that every write to the append-only file must use
`write()` or `writev()` for a complete page as described below. If you do not properly structure these writes you
can end up with an unreadable CADB file if your process is abruptly terminated. To avoid this database corruption
implementations **MUST** guarantee that complete page files are written atomically disc.

Page File Format Example:

```
+---------------+
| Block Header  |
+---------------+
| Block Header  |
+---------------+
| Leaf Header   |
+---------------+
| Block Header  |
+---------------+
| Branch Header |
+---------------+
|    DBTAIL     |
+---------------+
```

A few terms:

* `POS` is an 8 byte BigUint64. This is used for all file position references.
* `Leaf` is a node containing the key/value pairs of the hash digest (key) and the `POS` of the block data (value)
* `Branch` is a node containing child nodes (either more branches or leaves). It is a list of key/value pairs of the first hash digest
  in the child and the `POS` of the child header data.

**All page file sections are optional and un-ordered (unless being compacted) except the `DBTAIL`.**

The `DBTAIL` is a single `POS` (8 byte Uint64) 

If you need to stream block data into the database before committing it you can do so by writing page files with your `Block Header`
entries followed by the current DBTAIL.

