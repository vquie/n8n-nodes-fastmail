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

The node now supports broad JMAP access with these resources:

- `Session` (load Fastmail JMAP session metadata)
- `Identity` (`Identity/get`)
- `Mailbox` (`Mailbox/get`, `Mailbox/set`)
- `Email` (`Email/query`, `Email/get`, `Email/set`)
- `Thread` (`Thread/query`, `Thread/get`)
- `Submission` (`EmailSubmission/set`)
- `Masked Email` (`MaskedEmail/get`, `MaskedEmail/set`)
- `Raw JMAP` (custom method name + JSON args + capabilities)

`Raw JMAP` is the escape hatch for methods not exposed as first-class operations yet. This makes the node usable for practically all Fastmail-exposed JMAP methods.

## Credentials

Use a Fastmail API token with the required scopes for the JMAP methods you call.

## Compatibility

Tested with:

- n8n 1.21.1

## Resources

- [n8n community nodes documentation](https://docs.n8n.io/integrations/community-nodes/)
- [Fastmail Developer API](https://www.fastmail.com/dev/)
- [JMAP (RFC 8620)](https://www.rfc-editor.org/rfc/rfc8620)
- [JMAP Mail (RFC 8621)](https://www.rfc-editor.org/rfc/rfc8621)

## Local development (Docker)

Use the helper script:

```bash
./testdata/run.sh
```

The script starts a `node:22-slim` container, installs dependencies, builds the node, installs `n8n`, and starts it on `http://localhost:5678`.
