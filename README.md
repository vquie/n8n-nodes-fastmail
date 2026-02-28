# n8n-nodes-fastmail


[n8n](https://n8n.io/) is a [fair-code licensed](https://docs.n8n.io/reference/license/) workflow automation platform.

[Installation](#installation)  
[Operations](#operations)  
[Credentials](#credentials)  
[Compatibility](#compatibility)  
[Resources](#resources)  

## Installation

Follow the [installation guide](https://docs.n8n.io/integrations/community-nodes/installation/) in the n8n community nodes documentation.

## Operations

This node currently supports:

- Fetching Fastmail JMAP session/account information
- Fetching identities from the primary mail account
- Optional filtering by identity email

## Credentials

Use a Fastmail API token with the scope required for JMAP access.


## Compatibility

Tested with:

- n8n 1.21.1

## Resources

* [n8n community nodes documentation](https://docs.n8n.io/integrations/community-nodes/)
* [Fastmail](https://www.fastmail.com/)

## Local development (Docker)

Use the helper script:

```bash
./testdata/run.sh
```

The script starts a `node:22-slim` container, installs dependencies, builds the node, installs `n8n`, and starts it on `http://localhost:5678`.
