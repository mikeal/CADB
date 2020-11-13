import { Page, Node, Entry, Leaf, Branch, compare, compaction } from '../src/types.js'
import { deepStrictEqual as same } from 'assert'
import { full as inmem } from '../src/cache.js'
import crypto from 'crypto'

const enc8 = (...args) => {
  const template = [ 255, 255, 255, 255, 255, 255, 255, 255 ]
  let i = 8 - args.length
  while (args.length) {
    template[i] = args.shift()
    i++
  }
  return new Uint8Array(template)
}
const encRange = (num, size=8) => {
  const template = [ ...Array(size).keys()].map(() => 255)
  template[7] = 256
  const buffers = []
  while (num > 0) {
    template[7] -= 1
    buffers.push(new Uint8Array(template))
    if (!template[7]) {
      template[7] = 256
      template[0] -= 1
    }
    num -= 1
  }
  return buffers
}

const cleanBytes = b => [ ...new Uint8Array(b.buffer, b.byteOffset, b.byteLength) ]
const clean = obj => {
  const copy = { ...obj }
  if (copy.digest) copy.digest = cleanBytes(copy.digest)
  if (copy.posBytes) copy.posBytes = cleanBytes(copy.posBytes)
  if (copy.lengthBytes) copy.lengthBytes = cleanBytes(copy.lengthBytes)
  // bytes are only present when deserialized from a single buffer
  delete copy.bytes
  if (copy.entries) {
    copy.entries = copy.entries.map(entry => ({
      digest: cleanBytes(entry.digest),
      pos: entry.pos,
      length: entry.length
    }))
  }
  return copy
}

export default async test => {
  // TODO: entry
  const roundtrip = (CLS, ...args) => {
    const obj = CLS.from(...args)
    const b = Buffer.concat(obj.encode())
    const node = CLS.parse(b)
    same(b, Buffer.concat(node.encode()))
    if (obj.entries) {
      const o = obj.entries[0]
      const n = node.entries[0]
    }
    same(clean(obj), clean(node))
    same(obj.branch, obj.branch)
    same(obj.leaf, obj.leaf)
  }
  test('entry roundtrip (64b length)', () => roundtrip(Entry, enc8(1), 0n, 8n))
  test('entry roundtrip (32b length)', () => roundtrip(Entry, enc8(1), 0n, 8))
  const entries = [ Entry.from(enc8(1), 0n, 8n) ]
  test('leaf roundtrip', () => roundtrip(Leaf, entries))
  test('branch roundtrip (closed)', () => roundtrip(Branch, entries, true))
  test('branch roundtrip (open)', () => roundtrip(Branch, entries, false))
  test('page create w/ one entry (closed)', async test => {
    const { write, read, getSize, cache } = inmem()
    const batch = [{ put: { digest: enc8(1), data: enc8(2) } }]
    const page = Page.create(batch)
    write(page.vector)
    const root = await Node.load(read, getSize(), cache)
    const data = await root.get(enc8(1), read, cache)
    same([...data], [...enc8(2)])
  })
  const bigpage = size => {
    test(`page create w/ ${size} entries ()`, async test => {
      const { write, read, getSize, cache } = inmem()
      const digests = encRange(size)
      const batch = digests.map(b => ({ put: { digest: b, data: b.slice(1) } }))
      const page = Page.create(batch)
      write(page.vector)
      const root = await Node.load(read, getSize(), cache)
      for (const digest of digests) {
        const data = await root.get(digest, read, cache)
        same([...data], [...digest.slice(1)])
      }
    })
  }
  bigpage(300)
  bigpage(10 * 1000)

  test('transaction(inserts): one entry at a time', async test => {
    const { write, read, getSize, cache, copy } = inmem()
    let batch = [{ put: { digest: enc8(1), data: Buffer.from([1, 1]) } }]
    let page = Page.create(batch)
    write(page.vector)
    let root = await Node.load(read, getSize(), cache)
    let data = await root.get(enc8(1), read, cache)
    same([...data], [...Buffer.from([1, 1])])

    const query = [ new Uint8Array([0]), new Uint8Array([...Array(33).keys()].map(() => 255)) ]

    let inserts = [ enc8(1) ]
    const insert = async (...digests) => {
      inserts = inserts.concat(digests).sort(compare)
      batch = digests.map(digest => ({ put: { digest, data: Buffer.from([1, 1]) } }))
      page = await Page.transaction({ batch, cursor: getSize(), root: page.root, read, cache })
      write(page.vector)
      root = await Node.load(read, getSize(), cache)
      const checks = [...inserts]
      for await (const entry of root.range(...query, read, cache)) {
        const expected = checks.shift()
        if (!expected) throw new Error('Too many results')
        const data = await entry.read(read)
        same([...data], [1, 1])
        same([...entry.digest], [...expected])
      }
      same(checks.length, 0)
      return root
    }

    // insert one to the right
    await insert(enc8(5, 4))

    // insert one in-between
    await insert(enc8(5, 3))

    // insert one to the left
    await insert(enc8(3, 1))

    // insert a split
    let branch = await insert(enc8(4, 0))
    same(branch.branch, true)
    same(branch.entries.length, 2)

    // insert on to the left of the left branch
    branch = await insert(enc8(1, 5))
    same(branch.branch, true)
    same(branch.entries.length, 2)

    // insert on to the middle of the left branch
    branch = await insert(enc8(2, 5))
    same(branch.branch, true)
    same(branch.entries.length, 2)

    // insert on to the right of the left branch
    branch = await insert(enc8(3, 5))
    same(branch.branch, true)
    same(branch.entries.length, 2)

    // insert to the right of the right branch
    branch = await insert(enc8(255, 255))
    same(branch.branch, true)
    same(branch.entries.length, 2)

    // insert in the middle of the right branch
    branch = await insert(enc8(255, 250))
    same(branch.branch, true)
    same(branch.entries.length, 2)

    const _root = page.root

    const tmprm = async (digest) => {
      const batch = [ { del: { digest } } ]
      const { write, read, getSize, cache } = await copy()
      const page = await Page.transaction({ batch, cursor: getSize(), root: _root, read, cache })
      write(page.vector)
      const node = await Node.load(read, page.pos + page.size, cache)
      const checks = inserts.filter(d => compare(d, digest) !== 0)
      for await (const entry of node.range(...query, read, cache)) {
        const expected = checks.shift()
        if (!expected) throw new Error('Too many results')
        const data = await entry.read(read)
        same([...data], [1, 1])
        same([...entry.digest], [...expected])
      }
      same(checks.length, 0)
      return page.root
    }

    // rm every individual digest
    for (const digest of inserts) {
      await tmprm(digest)
    }

    const rm = async digest => {
      const batch = [ { del: { digest } } ]
      page = await Page.transaction({ batch, cursor: getSize(), root: page.root, read, cache })
      write(page.vector)
      root = await Node.load(read, page.pos + page.size, cache)
      const checks = [...inserts]
      for await (const entry of root.range(...query, read, cache)) {
        const expected = checks.shift()
        if (!expected) throw new Error('Too many results')
        const data = await entry.read(read)
        same([...data], [1, 1])
        same([...entry.digest], [...expected])
      }
      same(checks.length, 0)
      return page.root
    }

    // rm every individual digest
    while (inserts.length) {
      await rm(inserts.pop())
    }
  })

  test('transaction: stress test', async test => {
    const { write, read, getSize, cache, copy } = inmem()
    let batch = [{ put: { digest: enc8(1), data: Buffer.from([ 1 ]) } }]
    let page = Page.create(batch)
    write(page.vector)
    let root = await Node.load(read, getSize(), cache)
    let data = await root.get(enc8(1), read, cache)
    same([...data], [ 1 ])

    const query = [ new Uint8Array([0]), new Uint8Array([...Array(33).keys()].map(() => 255)) ]

    const encRange = (num, size=8) => [...Array(num).keys()].map(() => crypto.randomBytes(size))

    let inserts = [ enc8(1) ]
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
      page = await Page.transaction({ batch, cursor: getSize(), root: page.root, read, cache })
      write(page.vector)
      root = await Node.load(read, getSize(), cache)
      const checks = [...inserts]
      for await (const entry of root.range(...query, read, cache)) {
        const expected = checks.shift()
        if (!expected) throw new Error('Too many results')
        const data = await entry.read(read)
        same([...data], [1])
        // same([...entry.digest], [...expected])
      }
      same(checks.length, 0)
      return root
    }

    // initial insert
    await insert(encRange(1000), [])

    let i = 0
    while (i < 100) {
      i++
      console.log({i})
      await insert(encRange(1000), []) //, inserts.slice(0, 500))
    }
  })
}
