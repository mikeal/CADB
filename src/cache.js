const full = () => {
  const buffers = {}
  const nodes = {}
  let cursor = 0n
  const read = (pos, length) => {
    if (nodes[pos]) {
      return nodes[pos]
    }
    let b = buffers[pos]
    if (!b) throw new Error(`Cache Error read(${pos})`)
    while (b.byteLength !== length) {
      b = Buffer.concat([b, buffers[pos + BigInt(b.byteLength)]])
      if (!b) throw new Error(`Cache Error read(${pos})`)
    }
    return b
  }
  const cache = (node, pos, length) => {
    nodes[pos] = node
  }

  const write = vector => {
    vector = [...vector]
    while (vector.length) {
      const part = vector.splice(0, 100000)
      for (const header of part) {
        if (header.encode) {
          cache(header, cursor)
        } else {
          buffers[cursor] = header
        }
        cursor += BigInt(header.byteLength)
      }
    }
  }
  return { write, read, cache, getSize: () => cursor }
}

export { full }
