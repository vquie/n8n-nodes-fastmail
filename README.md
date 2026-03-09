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

This node provides structured actions for working with Fastmail messages, labels, drafts, and threads.

### Message Actions

- Add label to message
- Delete a message
- Get a message
- Get many messages
- Mark a message as read
- Mark a message as unread
- Remove label from message
- Reply to a message
- Send a message

### Label Actions

- Create a label
- Delete a label
- Get label info
- Get many labels

### Draft Actions

- Create a draft
- Delete a draft
- Get a draft
- Get many drafts

### Thread Actions

- Add label to thread
- Delete a thread
- Get a thread
- Get many threads
- Remove label from thread
- Reply to a message
- Trash a thread
- Untrash a thread

### Live options

The node fetches available Fastmail data directly for dropdowns:

- Labels (mapped to Fastmail mailboxes)
- Identities

## Credentials

Use a Fastmail API token with the required scopes for the JMAP methods you call.

## Compatibility

Tested with:

- n8n 2.11.0

## Resources

- [n8n community nodes documentation](https://docs.n8n.io/integrations/community-nodes/)
- [Fastmail Developer API](https://www.fastmail.com/dev/)
- [JMAP (RFC 8620)](https://www.rfc-editor.org/rfc/rfc8620)
- [JMAP Mail (RFC 8621)](https://www.rfc-editor.org/rfc/rfc8621)

## Icon Attribution

- Node icon source: `Fastmail_icon_2019.svg` from Wikimedia Commons.
- Wikimedia marks this file as `PD-textlogo` (public domain for copyright in many jurisdictions).
- Fastmail name and logo may still be protected by trademark rights.
- Users are responsible for confirming trademark and branding permissions for their use case.

## Local development (Docker)

Use the helper script:

```bash
./testdata/run.sh
```

The script starts a `node:22-slim` container, installs dependencies, builds the node, installs `n8n`, and starts it on `http://localhost:5678`.
