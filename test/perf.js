import { Page, Node, Entry, Leaf, Branch, sortBatch, compaction, compare } from '../src/types.js'
import { deepStrictEqual as same } from 'assert'
import { full as inmem } from '../src/cache.js'
import cadb from '../src/index.js'
import crypto from 'crypto'
import tempfile from 'tempfile'

const encRange = (num, size=8) => [...Array(num).keys()].map(() => crypto.randomBytes(size))

const create = async (size, digestLength=8, valueLength) => {
  if (!valueLength) valueLength = digestLength
  console.log('encoding', size, 'entries')
  const { write, read, cache, getSize } = inmem()
  const digests = encRange(size, digestLength)
  let data = Buffer.alloc(valueLength)
  const batch = sortBatch(digests.map(b => ({ put: { digest: b, data } })))
  let start = Date.now()
  const stopwatch = () => {
    const diff = ( Date.now() - start ) / 1000
    start = Date.now()
    return diff
  }
  console.log('writing', size, 'entries')
  const page = Page.create(batch, 0, true)
  let time = stopwatch()
  console.log(`${ time }s to write ${ size } entries`)
  console.log(`${ Math.floor(size / time) } writes per second`)
  write(page.vector)
  const root = await Node.load(read, getSize(), cache)
  start = Date.now()

  const query = [ new Uint8Array([0]), new Uint8Array([...Array(33).keys()].map(() => 255)) ]
  let i = 0
  const reads = []
  for await (const entry of root.range(...query, read, cache)) {
    i++
    reads.push(entry.read(read))
  }
  const buffs = await Promise.all(reads)
  time = stopwatch()
  // data = [ ...data ]
  // buffs.forEach(b => same([...b.slice()], data))
  console.log(`${ time }s to perform ${ i } range query`)
  console.log(`${ Math.floor(i / time) } reads per second in range query`)
}

const mutations = async (range, iterations) => {
  const { write, read, getSize, cache, copy } = inmem()
  const digest = encRange(1)[0]
  let batch = [{ put: { digest, data: Buffer.from([ 1 ]) } }]
  let page = Page.create(batch)
  write(page.vector)
  let root = await Node.load(read, getSize(), cache)
  let data = await root.get(digest, read, cache)
  same([...data], [ 1 ])

  const query = [ new Uint8Array([0]), new Uint8Array([...Array(33).keys()].map(() => 255)) ]

  let inserts = [ digest ]
  const insert = async (puts, dels) => {
    inserts = inserts.concat(puts).sort(compare)
    inserts = inserts.filter(digest => {
      for (const del of dels) {
        if (compare(del, digest) === 0) return false
      }
      return true
    })
    const put = puts.map(digest => ({ put: { digest, data: Buffer.from([1]) } }))
    const del = dels.map(digest => ({ del: { digest } }))
    batch = [ ...put, ...del ]
    batch = batch.sort(({ put: { digest: a }}, { put: { digest: b }}) => compare(a, b))
    const start = Date.now()
    page = await Page.transaction({ batch, cursor: getSize(), root: page.root, read, cache, sorted: true })
    write(page.vector)
    const end = Date.now()
    return { start, end }
 }

  let i = 0
  console.log({range, iterations})
  while (i < iterations) {
    i++
    const { start, end } = await insert(encRange(range), [])
    console.log(`range(${ range }) insert at ${ Math.floor(range / ( end - start ) * 1000) }p/s`)
  }
}

const fileMutations = async (range, iterations) => {
  const f = tempfile('.cadb')
  const data = encRange(1, 1024)[0]
  let i = 0
  while (i < iterations) {
    const node = cadb(f)
    const batch = encRange(range, 32).map(digest => ({ put: { digest, data } }))
    const start = Date.now()
    await node.batch(batch)
    const end = Date.now()
    console.log(`range(${ range }) insert at ${ Math.floor(range / ( end - start ) * 1000) }p/s`)
  }
}

const run = async () => {
  /*
  await create(1000 * 10, 32)
  await create(1000 * 20, 32)
  await create(1000 * 100, 32)
  await create(1000 * 1000, 32)

  await mutations(100000, 40)

  */

  await fileMutations(100000, 10000)
}
run()
