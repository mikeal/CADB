import { createHash } from 'crypto'
import TOKENS from './tokens.js'

const hash = (...buffers) => {
  const hasher = createHash('sha256')
  buffers.forEach(b => hasher.update(b))
  return hasher.digest()
}

const slicer = chunk => (start, end) => {
  if (chunk.shallowSlice) {
    return chunk.shallowSlice(start, end)
  } else {
    return chunk.subarray(start, end)
  }
}

const uint32 = b => {
  if (b.shallowSlice) b = b.slice()
  return Buffer.from(b.buffer, b.byteOffset, b.byteLength).readUint32LE()
}
const uint64 = b => {
  if (b.shallowSlice) b = b.slice()
  return Buffer.from(b.buffer, b.byteOffset, b.byteLength).readBigUint64LE()
}

// TODO: cache small numbers to avoid unnecessary tiny allocations
const enc32 = num => {
  const b = Buffer.allocUnsafe(4)
  b.writeUint32LE(num)
  return b
}
const enc64 = num => {
  const b = Buffer.allocUnsafe(8)
  b.writeBigUint64LE(num)
  return b
}

const to8 = b => Buffer.from(b)

const compare = (b1, b2) => {
  // Note: last perf profile of mutations showed that this function
  // is LazyCompile and using up a lot of the time. with some tweaking
  // this can probably get inlined
  for (let i = 0; i < b1.byteLength; i++) {
    if (b2.byteLength === i) return 1
    const c1 = b1[i]
    const c2 = b2[i]
    if (c1 === c2) continue
    if (c1 > c2) return 1
    else return -1
  }
  if (b2.byteLength > b1.byteLength) return -1
  return 0
}

class Entry {
  constructor ({ digest, pos, length, posBytes, lengthBytes }) {
    this.digest = digest
    this.pos = pos
    this.length = length
    this.posBytes = posBytes
    this.lengthBytes = lengthBytes
  }

  get isEntry () {
    return true
  }

  get byteLength () {
    return this.digest.byteLength + 12
  }

  get size () {
    return this.digest.size
  }

  encode () {
    return [this.digest, this.posBytes, this.lengthBytes]
  }

  read (readBytes) {
    return readBytes(this.pos, this.length)
  }

  static from (digest, pos, length) {
    length = Number(length)
    const posBytes = enc64(pos)
    const lengthBytes = enc32(length)
    return new Entry({ digest, pos, length, posBytes, lengthBytes })
  }

  static parse (bytes) {
    bytes = to8(bytes)
    let cursor = 0
    const size = bytes.byteLength - 12
    const slice = slicer(bytes)
    const digest = slice(0, size)
    cursor += size
    const posBytes = slice(cursor, cursor + 8)
    const pos = uint64(posBytes)
    cursor += 8
    const lengthBytes = slice(cursor, cursor + 4)
    const length = uint32(lengthBytes)
    return new Entry({ digest, pos, length, posBytes, lengthBytes })
  }
}

const parser = bytes => {
  bytes = to8(bytes)
  const slice = slicer(bytes)
  const [token] = bytes
  const info = TOKENS[token]
  const { size } = info
  if (size === 'VAR') throw new Error('Not implemented')
  let pos = 1
  const entries = []
  while (pos < bytes.byteLength) {
    const chunk = slice(pos, pos + size + 12)
    pos += (size + 12)
    entries.push(Entry.parse(chunk))
  }
  const parsed = { info, entries, bytes }
  if (info.leaf) {
    return new Leaf(parsed)
  } else {
    return new Branch(parsed)
  }
}

const _parsedRead = (node, pos, length, cache) => {
  if (node instanceof Uint8Array) {
    node = parser(node)
    cache(node, pos, length)
  }
  return node
}

const parsedRead = (read, pos, length, cache) => {
  const node = read(pos, length)
  if (node.then) return node.then(n => _parsedRead(n, pos, length, cache))
  else return _parsedRead(node, pos, length, cache)
}

class Node {
  constructor ({ info, entries, bytes }) {
    this.info = info
    this.entries = entries
    this.bytes = bytes
  }

  get isNode () {
    return true
  }

  get byteLength () {
    if (this.bytes) return this.bytes.byteLength
    if (this.info.size === 'VAR') throw new Error('Not implemented')
    return 1 + this.entries.map(entry => entry.byteLength).reduce(sum, 0)
  }

  get leaf () {
    return !!this.info.leaf
  }

  get branch () {
    return !!this.info.branch
  }

  static parse (bytes) {
    return parser(bytes)
  }

  hash () {
    if (this._hash) return this._hash
    this._hash = hash(...this.encode())
    return this._hash
  }

  encode () {
    if (this.bytes) return [this.bytes]
    if (this.info.size === 'VAR') throw new Error('Not implemented')
    else {
      return [this.info.bytes, ...this.entries.map(entry => entry.encode()).flat()]
    }
  }

  static create (read, pos, length, cache) {
    return parsedRead(read, pos, length, cache)
  }

  static async load (read, size, cache) {
    size = BigInt(size)
    const chunk = await read(size - 12n, 12)
    const slice = slicer(chunk)
    const [pos, length] = [uint64(slice(0, 8)), uint32(slice(8))]
    return parsedRead(read, pos, length, cache)
  }
}

const getSize = entries => {
  // empty leaf is 8 bytes long
  if (!entries[0]) return 8
  const size = entries[0].digest.byteLength
  let i = 1
  while (i < entries.length) {
    if (size !== entries[i].digest.byteLength) throw new Error('Not implemented')
    i += 1
  }
  return size
}

const leafRangeQuery = function * (entries, start, end, read) {
  for (const entry of entries) {
    const { digest } = entry
    if (compare(digest, start) >= 0) {
      if (compare(digest, end) >= 0) {
        break
      }
      yield entry
    }
  }
}

const branchRangeQuery = async function * (entries, start, end, read, cache) {
  const reads = []
  for (const entry of entries) {
    const { digest } = entry
    if (compare(digest, start) >= 0) {
      if (compare(digest, end) >= 0) {
        break
      }
      reads.push(parsedRead(read, entry.pos, entry.length, cache))
    }
  }
  for (let reader of reads) {
    reader = await reader
    yield * reader.range(start, end, read, cache)
  }
}

const _digest = x => x.put ? x.put.digest : x.del.digest
const sortBatch = batch => batch.sort((x, y) => compare(_digest(x), _digest(y)))

/*
console.log(compare(new Uint8Array([0]), new Uint8Array([1])))
console.log(compare(new Uint8Array([1]), new Uint8Array([0])))
*/

class Leaf extends Node {
  closed () {
    const { digest } = this.entries[this.entries.length - 1]
    return digest[digest.length - 1] === 0
  }

  get (digest, read) {
    for (const entry of this.entries) {
      if (compare(entry.digest, digest) === 0) {
        return entry.read(read)
      }
    }
    throw new Error('Not found')
  }

  range (start, end, read) {
    return leafRangeQuery(this.entries, start, end, read)
  }

  transaction (batch, read, cache, eject, write, sorted = false) {
    if (!sorted) batch = sortBatch(batch)
    else batch = [...batch]

    let entries = [...this.entries]

    const expected = {
      puts: batch.filter(op => op.put).length,
      dels: batch.filter(op => op.del).length
    }

    for (const { put, del } of batch) {
      if (del) {
        const findIndex = () => entries.findIndex(entry => compare(entry.digest, del.digest) === 0)
        let i = findIndex()
        // safety: this won't have to be a while loop once we can trust the writer a little more
        while (i !== -1) {
          entries.splice(i, 1)
          i = findIndex()
        }
      } else {
        const entry = Entry.from(put.digest, ...write(put.data))
        entries.push(entry)
      }
    }

    entries = entries.sort(({ digest: a }, { digest: b }) => compare(a, b))

    const chunks = []
    let chunk = []
    for (const entry of entries) {
      chunk.push(entry)
      if (entry.digest[entry.digest.length - 1] === 0) {
        chunks.push(chunk)
        chunk = []
      }
    }
    if (chunk.length) chunks.push(chunk)
    return chunks.map(entries => Leaf.from(entries))
  }

  has (digest) {
    for (const entry of this.entries) {
      if (compare(entry.digest, digest) === 0) {
        return true
      }
    }
    return false
  }

  static from (entries) {
    const size = getSize(entries)
    const info = TOKENS.leaf[size]
    if (typeof info === 'undefined') throw new Error('Not implemented')
    return new Leaf({ info, entries })
  }
}

const _mergeEntries = ({ entries, write, parentClosed }) => {
  if (!entries.length) return []
  // chunker
  const branches = []
  const handler = (chunk, closed) => {
    chunk = chunk.map(entry => {
      if (entry.isEntry) return entry
      const addr = write(entry)
      if (!entry.entries[0]) console.log({emptyBug: [], chunk, entry})
      return Entry.from(entry.entries[0].digest, ...addr)
    })
    if (!chunk.length) return
    const branch = Branch.from(chunk, closed)
    branches.push(branch)
  }
  let chunk = []
  for (const entry of entries) {
    if (!entry.isEntry) {
      const hash = entry.hash()
      if (hash[hash.length - 1] === 0) {
        handler(chunk, true)
        chunk = []
      }
    }
    chunk.push(entry)
  }
  if (chunk.length) {
    if (entries[entries.length - 1].isEntry) {
      handler(chunk, parentClosed)
    } else {
      handler(chunk, false)
    }
  }
  return branches
}

const mergeEntries = ({ entries, write, read, cache, eject, parentClosed }) => {
  entries = entries.flat()

  let i = 0
  // we need to collect merged branches as well as any new
  // branch reads. they are collected into a linear array
  // so that we can use full concurrency but remain synchronous
  // when no async is needed (saves time spent in the event loop)
  const pending = []
  while (i < entries.length) {
    // if there's a new branch that is not closed we need to merge it
    // with the chunk to the right.
    if (entries[i].isNode && !entries[i].closed() && entries.length > (i + 1)) {
      let [a, b] = entries.splice(i, 2)
      if (b.isEntry) {
        pending.push(a)
        const p = parsedRead(read, b.pos, b.length, cache)
        if (p.then) {
          pending.push(p)
          eject(b)
          i++
          break
        } else {
          b = p
        }
        // the referenced node is going to be modified
        // so this reference will be orphaned
      }
      // stick the merged branch back into the array to be re-processed
      // for another potential merge
      const all = a.entries.concat(b.entries)
      if (a.leaf || b.leaf) {
        if (a.leaf !== b.leaf) throw new Error('Not implemented')
        entries.splice(i, 0, Leaf.from(all))
      } else {
        entries.splice(i, 0, Branch.from(all, b.closed()))
      }
    } else {
      pending.push(entries[i])
      i++
    }
  }
  const args = { write, read, cache, eject, parentClosed }
  for (const entry of pending) {
    if (entry.then) return Promise.all(pending).then(entries => mergeEntries({ entries, ...args }))
  }
  return _mergeEntries({ entries, write, parentClosed })
}

class Branch extends Node {
  closed () {
    return this.info.closed
  }

  async get (digest, read, cache) {
    let last
    for (const entry of this.entries) {
      const comp = compare(digest, entry.digest)
      if (comp < 0) {
        break
      }
      last = entry
    }
    if (!last) throw new Error('Not found')
    const _node = parsedRead(read, last.pos, last.length, cache)
    if (_node.then) return _node.then(node => node.get(digest, read))
    return _node.get(digest, read)
  }

  range (start, end, read, cache) {
    return branchRangeQuery(this.entries, start, end, read, cache)
  }

  transaction (batch, read, cache, eject, write, sorted = false) {
    if (!sorted) batch = sortBatch(batch)
    else batch = [...batch]
    const results = []

    for (let i = 0; i < this.entries.length; i++) {
      // work backwards over the entries
      const entry = this.entries[this.entries.length - (i + 1)]
      const ops = []
      while (batch.length) {
        const digest = _digest(batch[batch.length - 1])
        if (compare(digest, entry.digest) > -1) {
          ops.push(batch.pop())
        } else {
          break
        }
      }
      if (i === (this.entries.length - 1) && batch.length) {
        batch.forEach(op => ops.push(op))
      }
      if (ops.length) {
        const run = node => node.transaction(ops, read, cache, eject, write, true)
        const node = parsedRead(read, entry.pos, entry.length, cache)
        if (node.then) results.push(node.then(node => run(node)))
        else results.push(run(node))
      } else {
        results.push(entry)
      }
    }
    const entries = results.reverse()

    const args = { write, read, cache, eject, parentClosed: this.info.closed }
    if (entries.length) eject(this)
    for (const header of entries) {
      if (header.then) {
        return Promise.all(entries).then(entries => {
          return mergeEntries({ entries, ...args })
        })
      }
    }
    return mergeEntries({ entries, ...args })
  }

  static from (entries, closed) {
    const size = getSize(entries)
    const info = TOKENS.branch[closed ? 'closed' : 'open'][size]
    return new Branch({ info, entries })
  }
}

const compactNode = async function * (node, read, cache) {
  for (const entry of node.entries) {
    if (node.leaf) {
      yield read(entry.pos, entry.length)
    } else {
      yield * compactNode(await parsedRead(read, entry.pos, entry.length, cache))
    }
  }
  yield node
}

const sum = (x, y) => x + y

const compaction = async function * (page, read, cache) {
  const root = await parsedRead(read, page.root.pos, page.root.length, cache)
  let i = 0
  let pos
  let length
  for await (const part of compactNode(root)) {
    if (part instanceof Uint8Array) {
      i += part.byteLength
      yield part
    } else {
      const encoded = part.encode()
      pos = i
      length = encoded.map(b => b.byteLength).reduce(sum)
      yield * encoded
      i += length
    }
  }
  yield enc64(pos)
  yield enc32(length)
}

const writer = (cursor) => {
  cursor = BigInt(cursor)
  const vector = []
  let size = 0n
  const write = (header) => {
    vector.push(header)
    let length = header.byteLength
    const addr = [cursor, length]
    length = BigInt(length)
    cursor += length
    size += length
    return addr
  }

  return { write, vector, pos: cursor, getSize: () => size }
}

class Page {
  constructor ({ vector, root, size, pos }) {
    this.vector = vector
    this.size = size
    this.root = root
    this.pos = pos
  }

  encode () {
    return this.vector.map(b => b.encode ? b.encode() : b).flat()
  }

  static async transaction ({ batch, cursor, root, read, cache, sorted }) {
    const tip = await parsedRead(read, ...root, cache)
    const ejected = []
    const eject = node => ejected.push(node)
    const { write, vector, getSize } = writer(cursor)
    let branches = await tip.transaction(batch, read, cache, eject, write, sorted)
    const args = { read, cache, eject, write, parentClosed: false }
    while (branches.length > 1) {
      branches = await mergeEntries({ entries: branches, ...args })
    }

    if (!branches.length) {
      root = write(Leaf.from([]))
    } else {
      root = write(branches[0])
    }

    write(enc64(root[0]))
    write(enc32(Number(root[1])))
    return new Page({ vector: vector.flat(), root, pos: cursor, size: getSize() })
  }

  static create (batch, cursor = 0n, sorted = false) {
    if (!sorted) batch = sortBatch(batch)
    const { write, vector, getSize } = writer(cursor)

    let entries = []
    let nodes = []
    let root

    const writeLeaf = () => {
      const leaf = Leaf.from(entries)
      root = write(leaf)
      nodes.push([leaf, Entry.from(entries[0].digest, ...root)])
      entries = []
    }
    for (const { put, del } of batch) {
      if (del) continue // noop, del on empty database
      const { digest, data } = put
      const [pos, length] = write(data)
      entries.push(Entry.from(digest, pos, length))
      if (digest[digest.byteLength - 1] === 0) {
        writeLeaf()
      }
    }
    if (entries.length) {
      writeLeaf()
    }

    const writeBranch = (closed) => {
      if (!entries.length) throw new Error('empty bug')
      const branch = Branch.from(entries, closed)
      root = write(branch)
      nodes.push([branch, Entry.from(entries[0].digest, ...root)])
      entries = []
    }

    while (nodes.length > 1) {
      const branches = nodes
      nodes = []
      for (const [node, entry] of branches) {
        entries.push(entry)
        const hash = node.hash()
        if (!hash[hash.byteLength - 1]) {
          writeBranch(true)
        }
      }
      if (entries.length) {
        writeBranch(false)
      }
    }

    const [pos, length] = root
    write(enc64(pos))
    write(enc32(Number(length)))
    return new Page({ vector: vector.flat(), root, pos: cursor, size: getSize() })
  }
}

export { Page, Entry, Leaf, Branch, Node, compaction, compare, sortBatch }
