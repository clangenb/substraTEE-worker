
# Worker design
![Design](./design.svg)

The design is motivated by substrate's service design. Making the worker generic allows to easily replace the `node_api`,
`server` or `enclave` when the circumstances demand that. Which is helpful in tests, and testdriven development.