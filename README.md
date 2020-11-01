# Content Address DataBase

Experimental single file content addressed database.

"Content address" means that data is keyed by a hash digest.

CADB is a single file database for storing content addressed block data. You can think of it
like a key/value store where the keys **MUST** be hash digests of a sufficient length and the
value is arbitrary binary data.

CADB is a special B+ tree which is both sorted and balanced using hash digests. The keys (digests) are
sorted by binary comparison and the tree's structure is chunked using randomness derived from the tail
of each hash. This produces a self-balancing and well sorted structure on-disc.

The benefits of this approach are:

* Predictable performance over time.
  * As the entries in the store increase the depth of the tree, the tree's shape scales and balances to the ideal new size.
  * The database never needs to be compacted in order to improve performance since the tree is
    iteratively balanced on mutation. Compaction only serves to remove orphaned data, read/write performance
    remains the same no matter how large the database file gets.
  * The depth of the tree can be used to estimate the size of the branch data in the top of the tree.
    This means branch reads can be stored in a simple LRU. This means there's no need to manually eject
    orphaned data or memmap the database file, the cache can predictably adjust to the needs of the tree.
* Portability
  * The database file can be copied at any time, sent around, and a new store can use it immediately with
    zero load time.
  * This combines a lot of the workflows we use CAR files for with our block storage needs into a tidy single
    package.
  * Since the tree is deterministally structured, a **compacted** CADB file can have guaranteed hash consistency.
  * Since the database is guaranteed to be a single map keyed by digest, the hash of a CADB file can be used in a CID
    and potentially integrated into IPLD traversals.
    
## CADB Page File

Every write to disc **MUST** be a single page file. This means that every write to the append-only file must use
`write()` or `writev()` for a complete page as described below. If you do not properly structure these writes you
can end up with an unreadable CADB file if your process is abruptly terminated. To avoid this database corruption
implementations **MUST** guarantee that complete page files are written atomically disc.

Page File Format Example:

```
+---------------+
| Block Data    |
+---------------+
| Block Data    |
+---------------+
| Leaf Header   |
+---------------+
| Block Data    |
+---------------+
| Branch Header |
+---------------+
|    DBTAIL     |
+---------------+
```

A few terms:

* `POS` is an 8 byte BigUint64. This is used for all file position references.
* `LENGTH` is a single 4 byte Uint32. This means CADB cannot store a single block larger than 4GB.
* `ADDR` is an 8 byte `POS` followed by a 4 byte Uint32 for the `LENGTH` of a read.
* `Leaf` is a node containing the key/value pairs of the hash digest (key) and the `POS` of the block data (value)
* `Branch` is a node containing child nodes (either more branches or leaves). It is a list of key/value pairs of the first hash digest
  in the child and the `POS` of the child header data.

**All page file sections are optional and un-ordered (unless being compacted) except the `DBTAIL`.**

The `DBTAIL` is a single `ADDR` (12 byte pair of `POS` and `LENGTH`) pointing to either a leaf or branch header for the current root of the database.

If you need to stream block data into the database before committing it you can do so by writing page files with your `Block Data`
entries followed by the current DBTAIL.

### TOKEN_TABLE

* `Token` is a Uint8.
* `Type` is a block type, either `LEAF` or `BRANCH`.
* `Open/Closed` is only necessary in branch headers. It indicates whether or not the last child's has closed the
  node or not. This not needed in leaf nodes because the digest tail is used to determine when the node closes.
* `Size` is the size of the `DIGEST` in bytes.

```
+-----------------------------+
| T  | Type   | O/C    | SIZE |
+-----------------------------+

| 1  | LEAF   | N/A    | 8    |
| 2  | LEAF   | N/A    | 16   |
| 3  | LEAF   | N/A    | 32   |
| 4  | LEAF   | N/A    | 64   |
| 5  | LEAF   | N/A    | 128  |
| 6  | LEAF   | N/A    | VAR  |

| 7  | BRANCH | OPEN   | 8    |
| 8  | BRANCH | OPEN   | 16   |
| 9  | BRANCH | OPEN   | 32   |
| 10 | BRANCH | OPEN   | 64   |
| 11 | BRANCH | OPEN   | 128  |
| 12 | BRANCH | OPEN   | VAR  |

| 13 | BRANCH | CLOSED | 8    |
| 14 | BRANCH | CLOSED | 16   |
| 15 | BRANCH | CLOSED | 32   |
| 16 | BRANCH | CLOSED | 64   |
| 17 | BRANCH | CLOSED | 128  |
| 18 | BRANCH | CLOSED | VAR  |
+-----------------------------+
```

### Leaf and Branch Headers

There are two types of headers, one type for common fixed size digests and another for variably sized digests.

The basic parsing rules for headers are the same except for how to parse the individual entries.

```
FIXED_LENGTH

+--------------------------+
| TOKEN | ... ENTRIES |
+--------------------------+

VARIABLE_LENGTH

+----------------------------------------------+
| TOKEN | COUNT |...LENGTHS | ... LEAF_ENTRIES |
+----------------------------------------------+

ENTRY

+---------------+
| DIGEST | ADDR |
+---------------+ 
```

* `TOKEN` refers to the typing token (see [TOKEN_TABLE](#TOKEN_TABLE)).
* `DIGEST` is the hash digest key. Its size is determined either by the typing token or by the list of lengths
  defined in the header in the case of a variable length leaf.
* Variable Length Digest Headers
  * `COUNT` the number of entries in the digest. Given the `COUNT` you can parse `LENGTHS` since every length is a fixed size of 4 bytes.
  * `LENGTHS` is an ordered Uint32 list of `DIGEST` lengths. You can parse the entries by adding 12 bytes to every length (`ADDR` is 12 bytes).

In a branch header, the `DIGEST` is the first digest in the child.

In a leaf header, the `DIGEST` is the hash digest key of the block data.

### Chunking

The database is a binary sorted tree of `DIGEST` keys. The leaf nodes need to be chunked into headers, and the branches that build
the sorted tree alos need to be chunked.

The algorithm used to chunking is deterministic. This means that we'll end up with the same tree structure regardless of the order in which
entries have been inserted, unlike a typical BTree. Ensuring this deterministic chunking means a little more work when we mutate the tree,
but it also means the tree remains balanced without the need to compact the database.

Since hash digests ensure randomization, we use the tail of these hashes in order to determine where each to chunk entries into a single header.

If a `DIGEST` in a **leaf header** ends in Uint8 `0` it closes the leaf header.

When creating **branch headers** we hash the child header and if it ends in Uint8 `0` it closes the branch header. In this case, a `TOKEN` that
is `CLOSED` must be used for the branch. The last branch header at every layer of depth may end in with an entry for a child branch that does not
close and must therefor use an `UNCLOSED`.

The reason we use these `CLOSED`/`UNCLOSED` tokens is so that we can mutate branch data without reading the tail data and re-hashing it and without
storing the hash of every child in every branch.

#### Mutations

The tree manipulation is relatively straightforward if you're familiar with BTree manipulation except for managing the consistent chunking.

Whenever you mutate a header you'll need:

* `UNCLOSED` headers need to be re-chunked on any mutation.
* Or, if the header is `CLOSED` and a new entry is appended to the end of the header or the last entry is removed, the header entries must be concatenated with
  the tree sibling to the right and re-chunked.
* Or, if the the last entry remains unchanged and the header is `CLOSED` any new child entries must be checked to see if they close the chunk and the header
  may need to be split.
  
## Compaction

One goal of compaction is that the resulting database file byte matches any other compacted store from any other implementation.

TODO: ordering rules.
