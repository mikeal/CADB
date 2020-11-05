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

const isFloat = n => Number(n) === n && n % 1 !== 0

// It's always faster to read numbers from their TypedArray
// but you can only read them out of properly aligned memory
// which is unpredictable. You can read them with DataView
// when they are unaligned, which is slower, but still faster
// than doing a memcopy

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
  let node = read(pos, length)
  if (node.then) return node.then(n => _parsedRead(node, pos, length, cache))
  else return _parsedRead(node, pos, length, cache)
}

class Node {
  constructor ({info, entries, bytes}) {
    this.info = info
    this.entries = entries
    this.bytes = bytes
  }

  get byteLength () {
    if (this.bytes) return this.bytes.byteLength
    if (this.info.size === 'VAR') throw new Error('Not implemented')
    return 1 + this.entries.map(entry => entry.byteLength).reduce(sum)
  }

  get leaf () {
    return !!this.info.leaf
  }

  get branch () {
    return !!this.info.branch
  }

  get closed () {
    return !!this.info.closed
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
    if (this.bytes) return [ this.bytes ]
    if (this.info.size === 'VAR') throw new Error('Not implemented')
    else {
      return [this.info.bytes, ...this.entries.map(entry => entry.encode()).flat()]
    }
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
  const size = entries[0].digest.byteLength
  let i = 1
  while (i < entries.length) {
    if (size !== entries[i].digest.byteLength) throw new Error('Not implemented')
    i += 1
  }
  return size
}

const leafRangeQuery = function * (entries, start, end, read) {
  const reads = []
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

class Leaf extends Node {
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

class Branch extends Node {
  async get (digest, read, cache) {
    let last
    let i = 0
    for (const entry of this.entries) {
      const comp = compare(digest, entry.digest)
      if (comp > 0) {
        break
      }
      last = entry
      i++
    }
    if (!last) throw new Error('Not found')
    const node = await parsedRead(read, last.pos, last.length, cache)
    return node.get(digest, read)
  }

  range (start, end, read, cache) {
    return branchRangeQuery(this.entries, start, end, read, cache)
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

const compaction = async function * (page, read) {
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

class Page {
  constructor ({ vector, root, size }) {
    this.vector = vector
    this.size = size
    this.root = root
  }

  tip (read, cache) {
    return parsedRead(read, ...this.root, cache)
  }

  static async transaction (batch, start, root, read) {
    const [pos, length] = root
    const node = await this.tip()
    // TODO
  }

  static create (batch, cursor = 0n) {
    // batch must already be sorted
    cursor = BigInt(cursor)
    let vector = []
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
      const branch = Branch.from(entries, closed)
      root = write(branch)
      nodes.push([branch, Entry.from(entries[0].digest, ...root)])
      entries = []
    }

    while (nodes.length > 1) {
      const branches = nodes
      nodes = []
      for (const [ node, entry ] of branches) {
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
    return new Page({ vector: vector.flat(), root, size })
  }
}

export { Page, Entry, Leaf, Branch, Node, compaction }
