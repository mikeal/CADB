import { Page, Node, compaction } from '../src/types.js'
import bl from 'bl'

const { BufferList } = bl

const file = () => {
  const bl = new BufferList()
  const write = (...buffers) => buffers.forEach(buffer => bl.append(buffer))
  const read = (pos, length) => {
    return bl.slice(Number(pos), Number(pos + BigInt(length)))
  }
  return { write, read, bl, getSize: () => bl.length }
}

const enc8 = i => new Uint8Array([255, 255, 255, 255, 255, 255, 255, i])

export default async test => {
  // TODO: entry
  // TODO: leaf
  // TODO: branch
  test('page create w/ one entry (closed)', async test => {
    const { write, read, bl, getSize } = file()
    const batch = [{ put: { digest: enc8(1), data: enc8(2) } }]
    const page = Page.create(batch)
    write(...page.vector)
    const root = await Node.load(read, getSize())
    console.log(root)
  })
}
