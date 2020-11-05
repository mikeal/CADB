import { Page, Node, Entry, Leaf, Branch, compaction } from '../src/types.js'
import bl from 'bl'

const { BufferList } = bl

const inmem = () => {
  const bl = new BufferList()
  const write = buffers => buffers.forEach(buffer => bl.append(buffer))
  const nodes = {}
  const read = (pos, length) => {
    if (nodes[pos] && nodes[pos][length]) {
      return nodes[pos][length]
    }
    const end = Number(pos + BigInt(length))
    const chunk = bl.shallowSlice(Number(pos), end)
    return chunk
  }
  const cache = (node, pos, length) => {
    if (!nodes[pos]) nodes[pos] = {}
    nodes[pos][length] = node
  }
  return { write, read, bl, cache, getSize: () => bl.length }
}

const enc8 = i => new Uint8Array([255, 255, 255, 255, 255, 255, 255, i])
const encRange = (num, size=8) => {
  const template = [...Array(size).keys()].map(() => 255)
  template[size - 1] = 256
  const buffers = []
  while (num > 0) {
    template[size - 1] -= 1
    buffers.push(new Uint8Array(template))
    if (!template[size - 1]) {
      template[7] = 256
      let i = 0
      while (template[i] === 0) {
        i++
      }
      template[i] -= 1
    }
    num -= 1
  }
  return buffers
}

const create = async (size, digestLength=8, valueLength) => {
  if (!valueLength) valueLength = digestLength
  console.log('encoding', size, 'entries')
  const { write, read, bl, cache, getSize } = inmem()
  const digests = encRange(size, digestLength)
  const data = Buffer.alloc(valueLength)
  const batch = digests.map(b => ({ put: { digest: b, data } }))
  let start = Date.now()
  const stopwatch = () => {
    const diff = ( Date.now() - start ) / 1000
    start = Date.now()
    return diff
  }
  console.log('writing', size, 'entries')
  const page = Page.create(batch)
  let time = stopwatch()
  console.log(`${ time }s to write ${ size } entries`)
  console.log(`${ Math.floor(size / time) } writes per second`)
  const vector = [...page.vector]
  while (vector.length) {
    const part = vector.splice(0, 100000)
    write(part)
  }
  const root = await Node.load(read, getSize(), cache)
  start = Date.now()
  /*
  for (const digest of digests) {
    await root.get(digest, read, cache)
  }
  time = stopwatch()
  console.log(`${ time }s to perform ${ size } reads`)
  console.log(`${ Math.floor(size / time) } reads per second`)
  */
}

const run = async () => {
  /*
  await create(1000)
  await create(1000 * 10)
  await create(1000 * 20)
  await create(1000 * 100)
  await create(1000 * 1000)

  await create(1000, 32)
  await create(1000 * 10, 32)
  await create(1000 * 20, 32)
  await create(1000 * 100, 32)
  await create(1000 * 1000, 32)
  */
  await create(1000 * 1000 * 1, 32, 1000)
}
run()
