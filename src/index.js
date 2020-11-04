import { writev as _writev, openSync, read as _read, statSync, readSync } from 'fs'
import { promisify } from 'util'

const writev = promisify(_writev)
const read = promisify(_read)

const mutex = filename => {
  // todo: create and check for .lockfile
  const writer = openSync(filename, 'a')
  const reader = openSync(filename, 'r')
  const { size } = statSync(filename, { bigint: true })
  let root = null

  if (size !== 0n) {
    const buffer = Buffer.allocUnsafe(12)
    readSync(reader, buffer, 0, 12, size - 12)
    root = [uint64(buffer.slice(0, 8)), uint32(buffer.slice(9))]
  }

  const get = async digest => {
    if (!root) throw new Error('Cannot read empty database')
  }

  let pending = []
  const write = (...buffers) => {
    pending = pending.concat(buffers)
  }

  const chunkLeafEntries = function * (entries) {
    let chunk = []
    for (const entry of entries) {
      const [digest] = entry
      chunk = chunk.concat(entry)
      if (digest[digest.length - 1] === 0) {
        // TODO: identify correct token based ok digest lengths
        yield [TOKENS.leaf[32].bytes, ...chunk]
      }
    }
    if (chunk.length) yield [TOKENS.leaf[32].bytes, ...chunk]
  }

  // await writev(writer, vector, start)

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
    const p = _transaction(batch)
    p.catch(clear)
    p.then(clear)
    lock = p
    return p
  }

  return { getSize: () => size }
}

export { mutex }
