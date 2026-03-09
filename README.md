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

This node is designed for broad usability (non-JSON-first UI) and now includes:

- `Email`
  - `List in Mailbox` (with live mailbox picker)
  - `Get by ID`
  - `Mark as Read`
  - `Mark as Unread`
  - `Delete`
  - `Send` (with live identity picker)
- `Mailbox`
  - `List`
  - `Get by ID` (with live mailbox picker)
- `Identity`
  - `List`
- `Session`
  - Session metadata
- `Raw JMAP (Advanced)`
  - Free method name + args for power users and edge methods

### Live options

The node fetches available Fastmail data directly for dropdowns:

- Mailboxes (`Mailbox/get`)
- Identities (`Identity/get`)

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
