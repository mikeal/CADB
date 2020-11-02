import { createHash } from 'crypto'
import { writev as _writev, openSync, read as _read, statSync, readSync } from 'fs'
import { promisify } from 'utils'

const writev = promisify(_writev)
const read = promisify(_read)

const hash = (...buffers) => {
  const hasher = createHash('sha256')
  buffers.forEach(b => hasher.update(b))
  return hasher.digest()
}

const dataview = bytes => new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength)
const uint32 = bytes => dataview(bytes).getUint32()
const uint64 = bytes => dataview(bytes).getBigUint64()
const _enc => (num, View) => {
  const view = new View(1)
  view[0] = num
  return view
}
// TODO: cache small numbers to avoid unnecessary tiny allocations
const enc8 = num => _enc(num, Uint8Array)
const enc32 = num => _enc(num, Uint32Array)
const enc64 = num => _enc(num, BigUint64Array)

const TOKENS = { leaf: {}, branch: { open: {}, closed: {} } }
const add = entry => {
  entry.bytes = enc8(token)
  const { token, leaf, branch, closed, size } = entry
  TOKENS[token] = entry
  if (branch) TOKENS.branch[closed ? 'closed' : 'open'][size] = entry
  if (leaf) TOKENS.leaf[size] = entry
}
/*
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
*/
add({ token: 1, leaf: true, size: 8 })
add({ token: 2, leaf: true, size: 16 })
add({ token: 3, leaf: true, size: 32 })
add({ token: 4, leaf: true, size: 64 })
add({ token: 5, leaf: true, size: 128 })
add({ token: 6, leaf: true, size: 'VAR' })

add({ token: 7, branch: true, open: true, size: 8 })
add({ token: 8, branch: true, open: true, size: 16 })
add({ token: 9, branch: true, open: true, size: 32 })
add({ token: 10, branch: true, open: true, size: 64 })
add({ token: 11, branch: true, open: true, size: 128 })
add({ token: 12, branch: true, open: true, size: 'VAR' })

add({ token: 12, branch: true, closed: true, size: 8 })
add({ token: 14, branch: true, closed: true, size: 16 })
add({ token: 15, branch: true, closed: true, size: 32 })
add({ token: 16, branch: true, closed: true, size: 64 })
add({ token: 17, branch: true, closed: true, size: 128 })
add({ token: 18, branch: true, closed: true, size: 'VAR' })



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

const encodeBranch = async () => {
}

const encodeAddress = (pos, length) => [ enc64(pos), enc32(length) ]

const mutex = filename => {
  // todo: create and check for .lockfile
  const writer = openSync(filename, 'a')
  const reader = openSync(filename, 'r')
  let { size } = statSync(filename, { bigint: true })
  let root = null

  if (size !== 0n) {
    const buffer = Buffer.allocUnsafe(12)
    readSync(reader, buffer, 0, 12, size - 12)
    root = [ uint64(buffer.slice(0, 8)), uint32(buffer.slice(9)) ]
  }

  const get = async digest => {
    if (!root) throw new Error('Cannot read empty database')
  }

  const addr = (...buffers) => {
    let length = 0
    const pos = size
    for (buffer of buffers) {
      length += buffer.byteLength
      size += buffer.byteLength
    }
    return [ pos, length ]
  }

  let pending = []
  const write = (...buffers) {
    pending = pending.concat(buffers)
  }

  const chunkLeafEntries = function * (entries) => {
    let chunk = []
    for (const entry of entries) {
      const [ digest ] = entry
      chunk = chunk.concat(entry)
      if (digest[digest.length - 1] === 0) {
        // TODO: identify correct token based ok digest lengths
        yield [ TOKENS.leaf[32].bytes, ...chunk ]
      }
    }
    if (chunk.length) yield [ TOKENS.leaf[32].bytes, ...chunk ]
  }

  const create = async batch => {
    size = 0
    let vector = []
    const entries = []
    for (const { put, del, digest, data } of batch) {
      if (del) continue // noop, del on empty database
      const [ pos, length ] = write(data)
      vector.push(data)
      entries.push([ digest,  ...encodeAddress(pos, length) ])
    }
    const headers = []
    for (let chunk of chunkLeafEntries(entries)) {
      const _hash = hash(...chunk)
      const [ pos, length ] = write(...chunk)
      vector = vector.concat(chunk)
      const [ , first ] = chunk
      headers.push([ first, ...encodeAddress(pos, length) ])
    }
    while (headers.length) {
      throw new Error('leaving here')
    }
  }

  const _transaction = async batch => {
    batch = batch.sort(({ digest: a }, { digest: b }) => compare(a, b))
    const buffer = []
    if (!root) {
      return create(batch)
    } else {
      while (batch.length) {
        const { put, del, digest, data } = batch.shift()
      }
    }
  }

  const transaction = async batch => {
    while (lock) {
      await lock()
    }
    let p = _transaction(batch)
    p.catch(clear)
    p.then(clear)
    lock = p
    return p
  }

  return { getSize: () => size }
}
