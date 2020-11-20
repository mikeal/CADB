import crypto from 'crypto'
import tempfile from 'tempfile'
import fs from 'fs'
import { deepStrictEqual as same } from 'assert'
import cadb from '../src/index.js'

const encRange = (num, size=8) => [...Array(num).keys()].map(() => crypto.randomBytes(size))

export default async test => {
  test('put and get', async test => {
    const f = tempfile('.cadb')
    // test.after(() => rm(f))
    const node = cadb(f)
    const [ digest, data ] = encRange(2)
    await node.put(digest, data)
    const data2 = await node.get(digest)
    same(data, data2)
  })
  test('batch', async test => {
    const f = tempfile('.cadb')
    // test.after(() => rm(f))
    const node = cadb(f)
    const data = encRange(1)[0]
    const batch = encRange(100).map(digest => ({ put: { digest, data } }))
    await node.batch(batch)
    for (const { put: { digest } } of batch) {
      same(data, await node.get(digest))
    }
  })
}
