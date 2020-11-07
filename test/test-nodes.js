import { Page, Node, Entry, Leaf, Branch, compare, compaction } from '../src/types.js'
import { deepStrictEqual as same } from 'assert'
import { full as inmem } from '../src/cache.js'

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
    const { write, read, getSize, cache } = inmem()
    let batch = [{ put: { digest: enc8(1), data: enc8(2) } }]
    let page = Page.create(batch)
    write(page.vector)
    let root = await Node.load(read, getSize(), cache)
    let data = await root.get(enc8(1), read, cache)
    same([...data], [...enc8(2)])

    const query = [ new Uint8Array([0]), new Uint8Array([...Array(33).keys()].map(() => 255)) ]

    let inserts = [ enc8(1) ]
    const insert = async (...digests) => {
      inserts = inserts.concat(digests).sort(compare)
      batch = digests.map(digest => ({ put: { digest, data: enc8(2) } }))
      console.log({before: getSize()})
      page = await Page.transaction({ batch, cursor: getSize(), root: page.root, read, cache })
      write(page.vector)
      console.log({after: getSize()})
      root = await Node.load(read, getSize(), cache)
      const checks = [...inserts]
      for await (const entry of root.range(...query, read, cache)) {
        const expected = checks.shift()
        if (!expected) throw new Error('Too many results')
        const data = await entry.read(read)
        same([...data], [...enc8(2)])
        same([...entry.digest], [...expected])
      }
      return root
    }

    // insert one to the right
    await insert(enc8(4))

    // insert one in-between
    await insert(enc8(3))
  })
}
