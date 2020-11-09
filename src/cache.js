const full = (buffers=new Map(), nodes=new Map(), cursor=0n) => {
  const read = (pos, length) => {
    if (nodes.has(pos)) {
      return nodes.get(pos)
    }
    let b = buffers.get(pos)
    if (!b) throw new Error(`Cache Error read(${pos})`)
    while (b.byteLength !== length) {
      b = Buffer.concat([b, buffers.get(pos + BigInt(b.byteLength))])
      if (!b) throw new Error(`Cache Error read(${pos})`)
    }
    return b
  }
  const cache = (node, pos, length) => {
    nodes.set(pos, node)
  }

  const write = vector => {
    vector = [...vector]
    while (vector.length) {
      const part = vector.splice(0, 100000)
      for (const header of part) {
        if (header.encode) {
          cache(header, cursor)
        } else {
          buffers.set(cursor, header)
        }
        cursor += BigInt(header.byteLength)
      }
    }
  }
  const copy = async () => full(new Map(buffers), new Map(nodes), cursor)
  return { write, read, cache, copy, getSize: () => cursor }
}

export { full }
