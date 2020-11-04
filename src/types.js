import { createHash } from 'crypto'
import TOKENS from './tokens.js'

const hash = (...buffers) => {
  const hasher = createHash('sha256')
  buffers.forEach(b => hasher.update(b))
  return hasher.digest()
}

const dataview = bytes => new DataView(bytes.buffer)
const uint32 = bytes => dataview(bytes).getUint32(bytes.byteOffset)
const uint64 = bytes => dataview(bytes).getBigUint64(bytes.byteOffset)
const _enc = (num, View) => {
  if (View !== BigUint64Array) num = Number(num)
  const view = new View(1)
  view[0] = num
  return view
}
// TODO: cache small numbers to avoid unnecessary tiny allocations
const enc32 = num => _enc(num, Uint32Array)
const enc64 = num => _enc(num, BigUint64Array)

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
    const posBytes = enc64(pos)
    const lengthBytes = enc32(length)
    return new Entry({ digest, pos, length, posBytes, lengthBytes })
  }

  static parse (size, bytes) {
    let cursor = 0
    const digest = bytes.subarray(0, size)
    cursor += size
    const posBytes = bytes.subarray(cursor, cursor + 8)
    const pos = uint64(posBytes)
    cursor += 8
    const lengthBytes = bytes.subarray(cursor, cursor + 4)
    const length = uint32(lengthBytes)
    return new Entry({ digest, pos, length, posBytes, lengthBytes })
  }
}

const parser = bytes => {
  const [token] = bytes
  const info = TOKENS[token]
  const { size } = info
  if (size === 'VAR') throw new Error('Not implemented')
  let pos = 1
  const entries = []
  while (pos < bytes.byteLength) {
    const chunk = bytes.subarray(pos, size + 12)
    pos += (size + 12)
    entries.push(Entry.parse(size, chunk))
  }
  const parsed = { info, entries, bytes }
  if (info.leaf) {
    return new Leaf(parsed)
  } else {
    return new Branch(parsed)
  }
}

const parsedRead = async (read, pos, length) => {
  const node = await read(pos, length)
  if (node instanceof Uint8Array) {
    return parser(node)
  }
  return node
}

class Node {
  constructor ({info, entries, bytes}) {
    this.info = info
    this.entries = entries
    this.bytes = bytes
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
    if (this.bytes) return [this.bytes]
    if (this.info.size === 'VAR') throw new Error('Not implemented')
    else {
      return [this.info.bytes, ...this.entries.map(entry => entry.encode()).flat()]
    }
  }

  static async load (read, size) {
    size = BigInt(size)
    const chunk = await read(size - 12n, size)
    const sub = chunk.subarray(0, 8)
    const [pos, length] = [uint64(chunk.subarray(0, 8)), uint32(chunk.subarray(9))]
    return parsedRead(read, pos, length)
  }
}

const getSize = entries => {
  const size = entries[0].digest.byteLength
  let i = 1
  while (i < entries.length) {
    if (size !== entries[i].digest.byteLength) throw new Error('Not implemented')
  }
  return size
}

class Leaf extends Node {
  get (digest, read) {
    for (const entry of this.entries) {
      if (compare(entry.digest, digest) === 0) {
        return entry.readBytes(read)
      }
    }
    throw new Error('Not found')
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
    const last = entries[entries.length - 1]
    const size = getSize(entries)
    const info = TOKENS.leaf[size]
    if (typeof info === 'undefined') throw new Error('Not implemented')
    return new Leaf({ info, entries })
  }
}
class Branch extends Node {
  get (digest, read) {
    // TODO
  }

  static from (entries, closed) {
    const size = getSize(entries)
    const info = TOKENS.branch[closed ? 'closed' : 'open'][size]
    return new Branch({ info, entries })
  }
}

const compactNode = async function * (node, read) {
  for (const entry of node.entries) {
    if (node.leaf) {
      yield read(entry.pos, entry.length)
    } else {
      yield * compactNode(await parsedRead(read, entry.pos, entry.length))
    }
  }
  yield node
}

const sum = (x, y) => x + y

const compaction = async function * (page, read) {
  const root = await parsedRead(read, page.root.pos, page.root.length)
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

  static async transaction (batch, start, root, read) {
    const [pos, length] = root
    const node = await parsedRead(read, pos, length)
    // TODO
  }

  static create (batch, cursor = 0n) {
    // batch must already be sorted
    cursor = BigInt(cursor)
    let vector = []
    let size = 0n
    const write = (...buffers) => {
      vector = vector.concat(buffers)
      const length = BigInt(buffers.map(b => b.byteLength).reduce(sum))
      const addr = [cursor, length]
      cursor += length
      size += length
      return addr
    }
    let entries = []
    let root
    for (const { put, del } of batch) {
      if (del) continue // noop, del on empty database
      const { digest, data } = put
      const [pos, length] = write(data)
      entries.push(Entry.from(digest, pos, length))
      if (digest[digest.byteLength - 1] === 0) {
        root = write(...Leaf.from(entries).encode())
        entries = []
      }
    }
    if (entries.length) {
      root = write(...Leaf.from(entries).encode())
    }
    const [pos, length] = root
    write(enc64(pos), enc32(length))
    return new Page({ vector, root, size })
  }
}

export { Page, compaction, Node }
