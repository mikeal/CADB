const TOKENS = { leaf: {}, branch: { open: {}, closed: {} } }
const add = entry => {
  const { token, leaf, branch, closed, size } = entry
  entry.bytes = Buffer.from([token])
  TOKENS[token] = entry
  if (branch) TOKENS.branch[closed ? 'closed' : 'open'][size] = entry
  if (leaf) TOKENS.leaf[size] = entry
}
add({ token: 1, leaf: true, size: 8 })
add({ token: 2, leaf: true, size: 16 })
add({ token: 3, leaf: true, size: 32 })
add({ token: 4, leaf: true, size: 64 })
add({ token: 5, leaf: true, size: 128 })
add({ token: 6, leaf: true, size: 'VAR' })

add({ token: 7, branch: true, open: true, size: 8 })
add({ token: 8, branch: true, open: true, size: 16 })
add({ token: 9, branch: true, open: true, size: 32 })
add({ token: 10, branch: true, open: true, size: 64 })
add({ token: 11, branch: true, open: true, size: 128 })
add({ token: 12, branch: true, open: true, size: 'VAR' })

add({ token: 12, branch: true, closed: true, size: 8 })
add({ token: 14, branch: true, closed: true, size: 16 })
add({ token: 15, branch: true, closed: true, size: 32 })
add({ token: 16, branch: true, closed: true, size: 64 })
add({ token: 17, branch: true, closed: true, size: 128 })
add({ token: 18, branch: true, closed: true, size: 'VAR' })

export default TOKENS
