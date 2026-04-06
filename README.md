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

This package provides:

- `Fastmail` for structured actions on messages, labels, drafts, and threads.
- `Fastmail Trigger` for near real-time new-message events via JMAP event stream (SSE with reconnect).

### Message Actions

- Add label to message
- Delete a message
- Forward a message
- Get a message
- Get many messages
- Mark a message as read
- Mark a message as unread
- Move a message
- Remove label from message
- Reply to a message
- Send a message

### Attachment Support

Attachments are supported as options on existing operations (not as separate actions):

- Upload from input binary properties on:
  - Message `Send`
  - Message `Forward`
  - Message `Reply`
  - Draft `Create`
  - Thread `Reply`
- Download to output binary on:
  - Message `Get`
  - Message `Get Many`

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

### UI behavior

- Optional fields are hidden by default.
- Reply and forward operations expose an `Auto Fill From Original` toggle.
  When enabled, the node keeps the current automatic behavior; when disabled, manual fields become available for values that would otherwise be derived from the original message.
- Use `Compose Options` to enable granular fields like `Cc`, `Bcc`, `Reply All`, `Create as Draft`, and attachment binary mapping.
- Use `Fetch Options` to enable granular fields like `Search`, `Read Status`, body values, and attachment download settings.
- `Mailbox Scope` and mailbox selection stay as dedicated top-level fields on `Get Many` (message/thread) for reliable mailbox picking.
- Invalid option combinations are rejected with explicit errors (for example, `Reply All` outside auto-filled reply operations or `Create as Draft` outside reply/forward operations).

## Credentials

Use a Fastmail API token with the required scopes for the JMAP methods you call.

### OAuth2 readiness

OAuth2 support is prepared in code and can be enabled with a simple toggle.

- Toggle constant:
  - `nodes/fastmail/Fastmail.node.ts` -> `ENABLE_FASTMAIL_OAUTH`
  - `nodes/fastmail/FastmailTrigger.node.ts` -> `ENABLE_FASTMAIL_OAUTH`
- Current default: `false` (API token mode only)
- Set to `true` to expose authentication switch (`API Token` / `OAuth2`) in the nodes.

Implemented OAuth2 credential:

- Credential type: `Fastmail OAuth2 API`
- Authorization URL: `https://www.fastmail.com/oauth/authorize`
- Token URL: `https://www.fastmail.com/oauth/token`
- Default scopes: `offline_access urn:ietf:params:jmap:core urn:ietf:params:jmap:mail urn:ietf:params:jmap:submission`

Reference:

- Fastmail Developer OAuth docs: [fastmail.com/dev](https://www.fastmail.com/dev/)

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

Local test run (simple Docker setup):

```bash
./testdata/run.sh
```

Prerequisite (once): run `npm ci` in the project root.
The script builds `dist`, prepares a clean custom package under `.testdata/custom`, and starts `n8nio/n8n`.
