import { writev as _writev, openSync, read as _read, statSync, readSync, readFileSync } from 'fs'
import { promisify } from 'util'
import { Page, Node } from './types.js'
import { full } from './cache.js'

import crypto from 'crypto'

const encRange = (num, size=8) => [...Array(num).keys()].map(() => crypto.randomBytes(size))

const writev = promisify(_writev)
const read = promisify(_read)

const uint32 = b => {
  if (b.shallowSlice) b = b.slice()
  return Buffer.from(b.buffer, b.byteOffset, b.byteLength).readUint32LE()
}
const uint64 = b => {
  if (b.shallowSlice) b = b.slice()
  return Buffer.from(b.buffer, b.byteOffset, b.byteLength).readBigUint64LE()
}

const mutex = filename => {
  // todo: create and check for .lockfile
  const writer = openSync(filename, 'a')
  const reader = openSync(filename, 'r')
  let { size } = statSync(filename, { bigint: true })
  let root = null

  if (size !== 0n) {
    const buffer = Buffer.allocUnsafe(12)
    readSync(reader, buffer, 0, 12, Number(size - BigInt(12)))
    root = [uint64(buffer.slice(0, 8)), uint32(buffer.slice(8))]
  }

  const cache = full()

  const _read = async (pos, length) => {
    const buffer = Buffer.allocUnsafe(length)
    await read(reader, buffer, 0, length, Number(pos))
    return buffer
  }

  const get = async digest => {
    if (!root) throw new Error('Cannot read empty database')
    const node = await Node.create(_read, ...root, cache.cache)
    const data = await node.get(digest, _read, cache.cache)
    return data
  }

  // await writev(writer, vector, start)

  const put = async (digest, data) => {
    const batch = [ { put: { digest, data } } ]
    let page
    if (!root) {
      page = await Page.create(batch)
    } else {
      const opts = { batch, cursor: size, root, read: _read, cache: cache.cache }
      page = await Page.transaction(opts)
    }
    const vector = page.encode()
    await writev(writer, vector, size)
    size = page.pos + page.size
    root = page.root
  }

  return { getSize: () => size, put, get }
}

export default mutex
