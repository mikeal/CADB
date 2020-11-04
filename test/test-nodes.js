import { Page, Node, Entry, Leaf, Branch, compaction } from '../src/types.js'
import { deepStrictEqual as same } from 'assert'
import bl from 'bl'

const { BufferList } = bl

const file = () => {
  const bl = new BufferList()
  const write = (...buffers) => buffers.forEach(buffer => bl.append(buffer))
  const read = (pos, length) => {
    const end = Number(pos + BigInt(length))
    const chunk = bl.slice(Number(pos), end)
    return chunk
  }
  return { write, read, bl, getSize: () => bl.length }
}

const enc8 = i => new Uint8Array([255, 255, 255, 255, 255, 255, 255, i])

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
    const bl = new BufferList(obj.encode())
    const node = CLS.parse(bl.slice())
    same(bl.slice(), (new BufferList(node.encode())).slice())
    if (obj.entries) {
      const o = obj.entries[0]
      const n = node.entries[0]
    }
    same(clean(obj), clean(node))
  }
  test('entry roundtrip (64b length)', () => roundtrip(Entry, enc8(1), 0n, 8n))
  test('entry roundtrip (32b length)', () => roundtrip(Entry, enc8(1), 0n, 8))
  const entries = [ Entry.from(enc8(1), 0n, 8n) ]
  test('leaf roundtrip', () => roundtrip(Leaf, entries))
  test('branch roundtrip (closed)', () => roundtrip(Branch, entries, true))
  test('branch roundtrip (open)', () => roundtrip(Branch, entries, false))
  test('page create w/ one entry (closed)', async test => {
    const { write, read, bl, getSize } = file()
    const batch = [{ put: { digest: enc8(1), data: enc8(2) } }]
    const page = Page.create(batch)
    write(...page.vector)
    const root = await Node.load(read, getSize())
    console.log(root)
  })
}
