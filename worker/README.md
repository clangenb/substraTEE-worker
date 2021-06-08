
# Worker design
![Design](./design.svg)

The design is motivated by substrate's service design. Making the worker generic allows to easily replace the `node_api`,
`server` or `enclave` with other implementations when circumstances demand that, i.e. testing or when downstream
projects require different implementations.