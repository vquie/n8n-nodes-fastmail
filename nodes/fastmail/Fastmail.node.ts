import {
  IExecuteFunctions,
  ILoadOptionsFunctions,
  INodeExecutionData,
  INodePropertyOptions,
  INodeType,
  INodeTypeDescription,
  NodeOperationError
} from 'n8n-workflow'

type JsonObject = Record<string, any>
type JmapMethodCall = [string, JsonObject, string]

interface SessionResponse {
  apiUrl: string
  username?: string
  accounts?: Record<string, { accountCapabilities?: Record<string, unknown> }>
  primaryAccounts?: Record<string, string>
}

interface JmapResponse {
  methodResponses?: Array<[string, JsonObject, string]>
}

interface MailboxRecord {
  id: string
  name?: string
  role?: string
  isSubscribed?: boolean
}

interface IdentityRecord {
  id: string
  email: string
  name?: string
}

interface EmailAddress {
  email: string
  name?: string
}

interface EmailRecord {
  id: string
  threadId?: string
  mailboxIds?: Record<string, boolean>
  from?: EmailAddress[]
  to?: EmailAddress[]
  cc?: EmailAddress[]
  subject?: string
  receivedAt?: string
  keywords?: Record<string, boolean>
  preview?: string
  messageId?: string[]
  inReplyTo?: string[]
  textBody?: Array<{ partId?: string }>
  htmlBody?: Array<{ partId?: string }>
  bodyValues?: Record<string, unknown>
}

interface ThreadRecord {
  id: string
  emailIds?: string[]
}

interface JmapSetResult {
  created?: Record<string, { id: string }>
  notCreated?: Record<string, unknown>
  updated?: Record<string, unknown>
  notUpdated?: Record<string, unknown>
  destroyed?: string[]
  notDestroyed?: Record<string, unknown>
}

const JMAP_CORE = 'urn:ietf:params:jmap:core'
const JMAP_MAIL = 'urn:ietf:params:jmap:mail'
const JMAP_SUBMISSION = 'urn:ietf:params:jmap:submission'

async function getSession (node: IExecuteFunctions | ILoadOptionsFunctions, token: string): Promise<SessionResponse> {
  return (await node.helpers.httpRequest({
    method: 'GET',
    url: 'https://api.fastmail.com/jmap/session',
    headers: {
      Authorization: `Bearer ${token}`
    },
    json: true
  })) as SessionResponse
}

function getPrimaryAccountId (session: SessionResponse, capability: string): string {
  const primary = session.primaryAccounts?.[capability]
  if (primary) return primary

  const discovered = Object.entries(session.accounts ?? {}).find(([, account]) =>
    Boolean(account.accountCapabilities?.[capability])
  )?.[0]
  if (discovered) return discovered

  throw new Error(`No account found for capability ${capability}`)
}

async function callJmap (
  node: IExecuteFunctions | ILoadOptionsFunctions,
  token: string,
  session: SessionResponse,
  using: string[],
  methodCalls: JmapMethodCall[]
): Promise<JmapResponse> {
  return (await node.helpers.httpRequest({
    method: 'POST',
    url: session.apiUrl,
    headers: {
      Authorization: `Bearer ${token}`
    },
    body: {
      using,
      methodCalls
    },
    json: true
  })) as JmapResponse
}

function methodResult<T = JsonObject> (response: JmapResponse, methodName: string): T {
  const record = response.methodResponses?.find(([name]) => name === methodName)
  if (record == null) throw new Error(`Missing method response for ${methodName}`)
  return record[1] as T
}

function parseCsvEmails (value: string): Array<{ email: string }> {
  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((email) => ({ email }))
}

function formatAddressList (addresses?: EmailAddress[]): string[] {
  return (addresses ?? []).map((entry) => entry.name ? `${entry.name} <${entry.email}>` : entry.email)
}

function getTokenFromCredentials (credentials: Record<string, unknown>): string {
  const token = credentials.token
  if (typeof token !== 'string' || token.trim() === '') {
    throw new Error('Fastmail API token is missing or invalid in credentials.')
  }
  return token
}

function simplifyEmail (email: EmailRecord, includeBodyValues = false): JsonObject {
  const simplified: JsonObject = {
    id: email.id,
    subject: email.subject ?? '',
    preview: email.preview ?? '',
    receivedAt: email.receivedAt ?? null,
    threadId: email.threadId ?? null,
    mailboxIds: Object.keys(email.mailboxIds ?? {}),
    from: formatAddressList(email.from),
    to: formatAddressList(email.to),
    cc: formatAddressList(email.cc),
    isRead: Boolean(email.keywords?.$seen),
    isDraft: Boolean(email.keywords?.$draft)
  }

  if (includeBodyValues) {
    const bodyValues = email.bodyValues ?? {}
    const textBody = extractBodyValue(email.textBody, bodyValues)
    const htmlBody = extractBodyValue(email.htmlBody, bodyValues)
    if (textBody != null) simplified.textBody = textBody
    if (htmlBody != null) simplified.htmlBody = htmlBody
  }

  return simplified
}

function firstCreatedId (result: JmapSetResult): string | null {
  const first = Object.values(result.created ?? {})[0]
  return first?.id ?? null
}

function extractBodyValue (parts: Array<{ partId?: string }> | undefined, bodyValues: Record<string, unknown>): string | null {
  if (parts == null || parts.length === 0) return null
  for (const part of parts) {
    const partId = part.partId
    if (partId == null || partId === '') continue
    const value = (bodyValues[partId] as { value?: string } | undefined)?.value
    if (typeof value === 'string' && value !== '') return value
  }
  return null
}

function getEmailProperties (includeBodyValues: boolean): string[] {
  const base = [
    'id',
    'threadId',
    'mailboxIds',
    'from',
    'to',
    'cc',
    'subject',
    'receivedAt',
    'keywords',
    'preview',
    'messageId',
    'inReplyTo'
  ]

  if (includeBodyValues) {
    base.push('textBody', 'htmlBody', 'bodyValues')
  }

  return base
}

function summarizeThread (thread: ThreadRecord, emailMap: Map<string, EmailRecord>, includeEmailIds: boolean): JsonObject {
  const emailIds = thread.emailIds ?? []
  const emails = emailIds
    .map((id) => emailMap.get(id))
    .filter((email): email is EmailRecord => email != null)
    .sort((a, b) => {
      const aTime = Date.parse(a.receivedAt ?? '')
      const bTime = Date.parse(b.receivedAt ?? '')
      return (Number.isNaN(bTime) ? 0 : bTime) - (Number.isNaN(aTime) ? 0 : aTime)
    })

  const latest = emails[0]
  const summary: JsonObject = {
    id: thread.id,
    messageCount: emailIds.length,
    unreadCount: emails.filter((email) => !Boolean(email.keywords?.$seen)).length,
    latestMessageSubject: latest?.subject ?? null,
    latestMessageFrom: latest?.from?.[0]?.email ?? null,
    latestMessageAt: latest?.receivedAt ?? null,
    latestMessagePreview: latest?.preview ?? null
  }

  if (includeEmailIds) {
    summary.emailIds = emailIds
  }

  return summary
}

async function getMailboxes (
  node: IExecuteFunctions | ILoadOptionsFunctions,
  token: string,
  session: SessionResponse,
  accountId: string
): Promise<MailboxRecord[]> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    ['Mailbox/get', { accountId, ids: null, properties: ['id', 'name', 'role', 'isSubscribed'] }, 'm1']
  ])
  return methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list ?? []
}

async function getMailboxIdByRole (
  node: IExecuteFunctions,
  token: string,
  session: SessionResponse,
  accountId: string,
  role: string
): Promise<string | null> {
  const mailboxes = await getMailboxes(node, token, session, accountId)
  return mailboxes.find((mailbox) => mailbox.role === role)?.id ?? null
}

async function getEmailById (
  node: IExecuteFunctions,
  token: string,
  session: SessionResponse,
  accountId: string,
  emailId: string,
  includeBodyValues = false
): Promise<EmailRecord | null> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    [
      'Email/get',
      {
        accountId,
        ids: [emailId],
        properties: getEmailProperties(includeBodyValues),
        fetchTextBodyValues: includeBodyValues,
        fetchHTMLBodyValues: includeBodyValues,
        fetchAllBodyValues: includeBodyValues
      },
      'e1'
    ]
  ])

  return methodResult<{ list?: EmailRecord[] }>(response, 'Email/get').list?.[0] ?? null
}

async function getEmailsByIds (
  node: IExecuteFunctions,
  token: string,
  session: SessionResponse,
  accountId: string,
  ids: string[],
  includeBodyValues = false
): Promise<EmailRecord[]> {
  if (ids.length === 0) return []

  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    [
      'Email/get',
      {
        accountId,
        ids,
        properties: getEmailProperties(includeBodyValues),
        fetchTextBodyValues: includeBodyValues,
        fetchHTMLBodyValues: includeBodyValues,
        fetchAllBodyValues: includeBodyValues
      },
      'e1'
    ]
  ])

  return methodResult<{ list?: EmailRecord[] }>(response, 'Email/get').list ?? []
}

async function getThreadEmailIds (
  node: IExecuteFunctions,
  token: string,
  session: SessionResponse,
  accountId: string,
  threadId: string
): Promise<string[]> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    ['Thread/get', { accountId, ids: [threadId] }, 't1']
  ])
  const thread = methodResult<{ list?: ThreadRecord[] }>(response, 'Thread/get').list?.[0]
  return thread?.emailIds ?? []
}

export class Fastmail implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Fastmail',
    name: 'fastmail',
    group: ['transform'],
    version: 1,
    icon: 'file:fastmail.svg',
    description: 'Actions for Fastmail messages, labels, drafts and threads',
    defaults: {
      name: 'Fastmail'
    },
    inputs: ['main'],
    outputs: ['main'],
    credentials: [
      {
        name: 'fastmailApi',
        required: true
      }
    ],
    properties: [
      {
        displayName: 'Resource',
        name: 'resource',
        type: 'options',
        noDataExpression: true,
        default: 'message',
        options: [
          { name: 'Message', value: 'message' },
          { name: 'Label', value: 'label' },
          { name: 'Draft', value: 'draft' },
          { name: 'Thread', value: 'thread' }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'getMany',
        displayOptions: {
          show: {
            resource: ['message']
          }
        },
        options: [
          { name: 'Add Label to Message', value: 'addLabel', action: 'Add a label to a message' },
          { name: 'Delete a Message', value: 'delete', action: 'Delete a message' },
          { name: 'Get a Message', value: 'get', action: 'Get a message' },
          { name: 'Get Many Messages', value: 'getMany', action: 'Get many messages' },
          { name: 'Mark a Message as Read', value: 'markRead', action: 'Mark a message as read' },
          { name: 'Mark a Message as Unread', value: 'markUnread', action: 'Mark a message as unread' },
          { name: 'Remove Label From Message', value: 'removeLabel', action: 'Remove a label from a message' },
          { name: 'Reply to a Message', value: 'reply', action: 'Reply to a message' },
          { name: 'Send a Message', value: 'send', action: 'Send a message' }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'getMany',
        displayOptions: {
          show: {
            resource: ['label']
          }
        },
        options: [
          { name: 'Create a Label', value: 'create', action: 'Create a label' },
          { name: 'Delete a Label', value: 'delete', action: 'Delete a label' },
          { name: 'Get a Label Info', value: 'get', action: 'Get label info' },
          { name: 'Get Many Labels', value: 'getMany', action: 'Get many labels' }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'getMany',
        displayOptions: {
          show: {
            resource: ['draft']
          }
        },
        options: [
          { name: 'Create a Draft', value: 'create', action: 'Create a draft' },
          { name: 'Delete a Draft', value: 'delete', action: 'Delete a draft' },
          { name: 'Get a Draft', value: 'get', action: 'Get a draft' },
          { name: 'Get Many Drafts', value: 'getMany', action: 'Get many drafts' }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'getMany',
        displayOptions: {
          show: {
            resource: ['thread']
          }
        },
        options: [
          { name: 'Add Label to Thread', value: 'addLabel', action: 'Add a label to a thread' },
          { name: 'Delete a Thread', value: 'delete', action: 'Delete a thread' },
          { name: 'Get a Thread', value: 'get', action: 'Get a thread' },
          { name: 'Get Many Threads', value: 'getMany', action: 'Get many threads' },
          { name: 'Remove Label From Thread', value: 'removeLabel', action: 'Remove a label from a thread' },
          { name: 'Reply to a Message', value: 'reply', action: 'Reply to a message in a thread' },
          { name: 'Trash a Thread', value: 'trash', action: 'Trash a thread' },
          { name: 'Untrash a Thread', value: 'untrash', action: 'Untrash a thread' }
        ]
      },
      {
        displayName: 'Message ID',
        name: 'messageId',
        type: 'string',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['message'],
            operation: ['get', 'delete', 'markRead', 'markUnread', 'addLabel', 'removeLabel', 'reply']
          }
        }
      },
      {
        displayName: 'Label Name or ID',
        name: 'labelId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getLabels'
        },
        description: 'Choose from the list, or set an ID with an expression. Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['message', 'thread', 'label'],
            operation: ['addLabel', 'removeLabel', 'get', 'delete']
          }
        }
      },
      {
        displayName: 'Limit',
        name: 'limit',
        type: 'number',
        default: 50,
        typeOptions: {
          minValue: 1
        },
        description: 'Max number of results to return',
        displayOptions: {
          show: {
            resource: ['message', 'draft', 'thread'],
            operation: ['getMany']
          }
        }
      },
      {
        displayName: 'Filter by Label Name or ID',
        name: 'filterLabelId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getLabels'
        },
        description: 'Optional label filter. Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        default: '',
        displayOptions: {
          show: {
            resource: ['message', 'thread'],
            operation: ['getMany']
          }
        }
      },
      {
        displayName: 'Include Body Values',
        name: 'includeBodyValues',
        type: 'boolean',
        default: false,
        displayOptions: {
          show: {
            resource: ['message', 'draft'],
            operation: ['get', 'getMany']
          }
        }
      },
      {
        displayName: 'From Identity Name or ID',
        name: 'identityId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getIdentities'
        },
        description: 'Choose from the list, or set an ID with an expression. Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        required: true,
        default: '',
        displayOptions: {
          show: {
            resource: ['message', 'draft', 'thread'],
            operation: ['send', 'reply', 'create']
          }
        }
      },
      {
        displayName: 'To',
        name: 'to',
        type: 'string',
        required: true,
        default: '',
        placeholder: 'alice@example.com,bob@example.com',
        description: 'Comma-separated recipient emails',
        displayOptions: {
          show: {
            resource: ['message', 'draft'],
            operation: ['send', 'create']
          }
        }
      },
      {
        displayName: 'Cc',
        name: 'cc',
        type: 'string',
        default: '',
        displayOptions: {
          show: {
            resource: ['message', 'draft'],
            operation: ['send', 'create']
          }
        }
      },
      {
        displayName: 'Bcc',
        name: 'bcc',
        type: 'string',
        default: '',
        displayOptions: {
          show: {
            resource: ['message', 'draft'],
            operation: ['send', 'create']
          }
        }
      },
      {
        displayName: 'Subject',
        name: 'subject',
        type: 'string',
        default: '',
        displayOptions: {
          show: {
            resource: ['message', 'draft'],
            operation: ['send', 'create']
          }
        }
      },
      {
        displayName: 'Text Body',
        name: 'textBody',
        type: 'string',
        default: '',
        typeOptions: {
          rows: 6
        },
        displayOptions: {
          show: {
            resource: ['message', 'draft', 'thread'],
            operation: ['send', 'create', 'reply']
          }
        }
      },
      {
        displayName: 'HTML Body',
        name: 'htmlBody',
        type: 'string',
        default: '',
        typeOptions: {
          rows: 6
        },
        displayOptions: {
          show: {
            resource: ['message', 'draft', 'thread'],
            operation: ['send', 'create', 'reply']
          }
        }
      },
      {
        displayName: 'Reply All',
        name: 'replyAll',
        type: 'boolean',
        default: false,
        displayOptions: {
          show: {
            resource: ['message', 'thread'],
            operation: ['reply']
          }
        }
      },
      {
        displayName: 'Draft ID',
        name: 'draftId',
        type: 'string',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['draft'],
            operation: ['get', 'delete']
          }
        }
      },
      {
        displayName: 'Thread ID',
        name: 'threadId',
        type: 'string',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['thread'],
            operation: ['get', 'delete', 'addLabel', 'removeLabel', 'trash', 'untrash']
          }
        }
      },
      {
        displayName: 'Include Message IDs',
        name: 'includeEmailIds',
        type: 'boolean',
        default: false,
        displayOptions: {
          show: {
            resource: ['thread'],
            operation: ['get', 'getMany']
          }
        }
      },
      {
        displayName: 'Message ID to Reply To',
        name: 'replyMessageId',
        type: 'string',
        default: '',
        required: true,
        description: 'Message ID within the thread to reply to',
        displayOptions: {
          show: {
            resource: ['thread'],
            operation: ['reply']
          }
        }
      },
      {
        displayName: 'Label Name',
        name: 'labelName',
        type: 'string',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['label'],
            operation: ['create']
          }
        }
      },
    ]
  }

  methods = {
    loadOptions: {
      async getLabels (this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const credentials = (await this.getCredentials('fastmailApi'))
        const token = getTokenFromCredentials(credentials)
        const session = await getSession(this, token)
        const accountId = getPrimaryAccountId(session, JMAP_MAIL)
        const mailboxes = await getMailboxes(this, token, session, accountId)

        return mailboxes
          .filter((mailbox) => mailbox.id)
          .map((mailbox) => ({
            name: mailbox.role ? `${mailbox.name ?? mailbox.id} (${mailbox.role})` : (mailbox.name ?? mailbox.id),
            value: mailbox.id
          }))
      },

      async getIdentities (this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const credentials = (await this.getCredentials('fastmailApi'))
        const token = getTokenFromCredentials(credentials)
        const session = await getSession(this, token)
        const accountId = getPrimaryAccountId(session, JMAP_SUBMISSION)

        const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
          ['Identity/get', { accountId, ids: null, properties: ['id', 'email', 'name'] }, 'i1']
        ])

        const identities = methodResult<{ list?: IdentityRecord[] }>(response, 'Identity/get').list ?? []
        return identities
          .filter((identity) => identity.id)
          .map((identity) => ({
            name: identity.name ? `${identity.name} <${identity.email}>` : identity.email,
            value: identity.id
          }))
      }
    }
  }

  async execute (this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData()
    const credentials = (await this.getCredentials('fastmailApi'))
    const token = getTokenFromCredentials(credentials)
    const returnData: INodeExecutionData[] = []

    for (let i = 0; i < items.length; i++) {
      try {
        const resource = this.getNodeParameter('resource', i)
        const operation = this.getNodeParameter('operation', i)

        const session = await getSession(this, token)
        const mailAccountId = getPrimaryAccountId(session, JMAP_MAIL)
        const submissionAccountId = getPrimaryAccountId(session, JMAP_SUBMISSION)

        if (resource === 'message') {
          if (operation === 'get') {
            const messageId = this.getNodeParameter('messageId', i) as string
            const includeBodyValues = this.getNodeParameter('includeBodyValues', i, false) as boolean
            const email = await getEmailById(this, token, session, mailAccountId, messageId, includeBodyValues)
            if (email == null) {
              returnData.push({ json: { message: 'Message not found', messageId }, pairedItem: { item: i } })
            } else {
              returnData.push({ json: simplifyEmail(email, includeBodyValues), pairedItem: { item: i } })
            }
            continue
          }

          if (operation === 'getMany') {
            const limit = this.getNodeParameter('limit', i, 25)
            const filterLabelId = this.getNodeParameter('filterLabelId', i, '') as string
            const includeBodyValues = this.getNodeParameter('includeBodyValues', i, false) as boolean

            const filter = filterLabelId ? { inMailbox: filterLabelId } : {}
            const queryResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/query', { accountId: mailAccountId, filter, sort: [{ property: 'receivedAt', isAscending: false }], limit }, 'q1']
            ])
            const ids = methodResult<{ ids?: string[] }>(queryResponse, 'Email/query').ids ?? []
            const emails = await getEmailsByIds(this, token, session, mailAccountId, ids, includeBodyValues)

            for (const email of emails) {
              returnData.push({ json: simplifyEmail(email, includeBodyValues), pairedItem: { item: i } })
            }
            continue
          }

          if (operation === 'delete' || operation === 'markRead' || operation === 'markUnread' || operation === 'addLabel' || operation === 'removeLabel') {
            const messageId = this.getNodeParameter('messageId', i) as string
            const update: JsonObject = {}

            if (operation === 'markRead') update['keywords/$seen'] = true
            if (operation === 'markUnread') update['keywords/$seen'] = false
            if (operation === 'addLabel') {
              const labelId = this.getNodeParameter('labelId', i) as string
              update[`mailboxIds/${labelId}`] = true
            }
            if (operation === 'removeLabel') {
              const labelId = this.getNodeParameter('labelId', i) as string
              update[`mailboxIds/${labelId}`] = null
            }

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              operation === 'delete'
                ? ['Email/set', { accountId: mailAccountId, destroy: [messageId] }, 's1']
                : ['Email/set', { accountId: mailAccountId, update: { [messageId]: update } }, 's1']
            ])

            const result = methodResult<JmapSetResult>(response, 'Email/set')
            returnData.push({
              json: {
                action: operation,
                messageId,
                successful: operation === 'delete' ? (result.destroyed ?? []).includes(messageId) : Object.keys(result.updated ?? {}).includes(messageId)
              },
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'send') {
            const identityId = this.getNodeParameter('identityId', i) as string
            const to = parseCsvEmails(this.getNodeParameter('to', i) as string)
            const cc = parseCsvEmails(this.getNodeParameter('cc', i, '') as string)
            const bcc = parseCsvEmails(this.getNodeParameter('bcc', i, '') as string)
            const subject = this.getNodeParameter('subject', i, '') as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string

            if (to.length === 0) {
              throw new NodeOperationError(this.getNode(), 'At least one recipient is required', { itemIndex: i })
            }

            const identityResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
              ['Identity/get', { accountId: submissionAccountId, ids: [identityId] }, 'i1']
            ])
            const identity = methodResult<{ list?: IdentityRecord[] }>(identityResponse, 'Identity/get').list?.[0]
            if (identity == null) {
              throw new NodeOperationError(this.getNode(), 'Selected identity was not found', { itemIndex: i })
            }
            const draftMailboxId = await getMailboxIdByRole(this, token, session, mailAccountId, 'drafts')
            if (draftMailboxId == null) {
              throw new NodeOperationError(this.getNode(), 'Drafts mailbox could not be found', { itemIndex: i })
            }

            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to,
              subject,
              keywords: { $draft: true },
              mailboxIds: { [draftMailboxId]: true }
            }
            if (cc.length > 0) createEmail.cc = cc
            if (bcc.length > 0) createEmail.bcc = bcc

            const bodyValues: Record<string, JsonObject> = {}
            if (textBody) {
              bodyValues.textPart = { value: textBody }
              createEmail.textBody = [{ partId: 'textPart', type: 'text/plain' }]
            }
            if (htmlBody) {
              bodyValues.htmlPart = { value: htmlBody }
              createEmail.htmlBody = [{ partId: 'htmlPart', type: 'text/html' }]
            }
            if (Object.keys(bodyValues).length > 0) createEmail.bodyValues = bodyValues

            const sendResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION], [
              ['Email/set', { accountId: mailAccountId, create: { draft: createEmail } }, 'c1'],
              ['EmailSubmission/set', { accountId: submissionAccountId, create: { submit: { identityId, emailId: '#draft' } } }, 's1']
            ])

            returnData.push({
              json: {
                success: true,
                  sentMessageId: (() => {
                    const emailSetResult = methodResult<JmapSetResult>(sendResponse, 'Email/set')
                    const createdId = firstCreatedId(emailSetResult)
                    if (createdId == null) {
                      const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
                      throw new NodeOperationError(this.getNode(), `Email was not created. notCreated: ${notCreated}`, { itemIndex: i })
                    }
                    const submissionResult = methodResult<JmapSetResult>(sendResponse, 'EmailSubmission/set')
                    if (firstCreatedId(submissionResult) == null) {
                      const notCreated = JSON.stringify(submissionResult.notCreated ?? {})
                      throw new NodeOperationError(this.getNode(), `Email submission failed. notCreated: ${notCreated}`, { itemIndex: i })
                    }
                    return createdId
                  })()
                },
                pairedItem: { item: i }
              })
            continue
          }

          if (operation === 'reply') {
            const messageId = this.getNodeParameter('messageId', i) as string
            const identityId = this.getNodeParameter('identityId', i) as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string
            const replyAll = this.getNodeParameter('replyAll', i, false) as boolean

            const original = await getEmailById(this, token, session, mailAccountId, messageId)
            if (original == null) {
              throw new NodeOperationError(this.getNode(), 'Original message not found', { itemIndex: i })
            }

            const identityResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
              ['Identity/get', { accountId: submissionAccountId, ids: [identityId] }, 'i1']
            ])
            const identity = methodResult<{ list?: IdentityRecord[] }>(identityResponse, 'Identity/get').list?.[0]
            if (identity == null) {
              throw new NodeOperationError(this.getNode(), 'Selected identity was not found', { itemIndex: i })
            }
            const draftMailboxId = await getMailboxIdByRole(this, token, session, mailAccountId, 'drafts')
            if (draftMailboxId == null) {
              throw new NodeOperationError(this.getNode(), 'Drafts mailbox could not be found', { itemIndex: i })
            }

            const recipients: EmailAddress[] = [...(original.from ?? [])]
            if (replyAll) {
              const addrs = [...(original.to ?? []), ...(original.cc ?? [])]
              for (const addr of addrs) {
                const exists = recipients.some((r) => r.email.toLowerCase() === addr.email.toLowerCase())
                if (!exists && addr.email.toLowerCase() !== identity.email.toLowerCase()) {
                  recipients.push(addr)
                }
              }
            }

            const subject = original.subject?.toLowerCase().startsWith('re:') ? (original.subject ?? '') : `Re: ${original.subject ?? ''}`
            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to: recipients,
              subject,
              keywords: { $draft: true },
              mailboxIds: { [draftMailboxId]: true }
            }

            if (original.messageId?.[0]) {
              createEmail.inReplyTo = [original.messageId[0]]
            }

            const bodyValues: Record<string, JsonObject> = {}
            if (textBody) {
              bodyValues.textPart = { value: textBody }
              createEmail.textBody = [{ partId: 'textPart', type: 'text/plain' }]
            }
            if (htmlBody) {
              bodyValues.htmlPart = { value: htmlBody }
              createEmail.htmlBody = [{ partId: 'htmlPart', type: 'text/html' }]
            }
            if (Object.keys(bodyValues).length > 0) createEmail.bodyValues = bodyValues

            const replyResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION], [
              ['Email/set', { accountId: mailAccountId, create: { replyDraft: createEmail } }, 'c1'],
              ['EmailSubmission/set', { accountId: submissionAccountId, create: { submit: { identityId, emailId: '#replyDraft' } } }, 's1']
            ])

            const emailSetResult = methodResult<JmapSetResult>(replyResponse, 'Email/set')
            const createdReplyId = firstCreatedId(emailSetResult)
            if (createdReplyId == null) {
              const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Reply could not be created. notCreated: ${notCreated}`, { itemIndex: i })
            }
            const submissionResult = methodResult<JmapSetResult>(replyResponse, 'EmailSubmission/set')
            if (firstCreatedId(submissionResult) == null) {
              const notCreated = JSON.stringify(submissionResult.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Reply submission failed. notCreated: ${notCreated}`, { itemIndex: i })
            }

            returnData.push({
              json: {
                success: true,
                replyMessageId: createdReplyId
              },
              pairedItem: { item: i }
            })
            continue
          }
        }

        if (resource === 'label') {
          if (operation === 'getMany') {
            const mailboxes = await getMailboxes(this, token, session, mailAccountId)
            for (const mailbox of mailboxes) {
              returnData.push({
                json: {
                  id: mailbox.id,
                  name: mailbox.name ?? mailbox.id,
                  role: mailbox.role ?? null,
                  isSubscribed: mailbox.isSubscribed ?? null
                },
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'get') {
            const labelId = this.getNodeParameter('labelId', i) as string
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Mailbox/get', { accountId: mailAccountId, ids: [labelId] }, 'm1']
            ])
            const mailbox = methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list?.[0]
            if (mailbox == null) {
              returnData.push({ json: { message: 'Label not found', labelId }, pairedItem: { item: i } })
            } else {
              returnData.push({
                json: {
                  id: mailbox.id,
                  name: mailbox.name ?? mailbox.id,
                  role: mailbox.role ?? null,
                  isSubscribed: mailbox.isSubscribed ?? null
                },
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'create') {
            const labelName = this.getNodeParameter('labelName', i) as string
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Mailbox/set', { accountId: mailAccountId, create: { newLabel: { name: labelName } } }, 'm1']
            ])
            const created = methodResult<JmapSetResult>(response, 'Mailbox/set')
            const createdLabelId = firstCreatedId(created)
            if (createdLabelId == null) {
              const notCreated = JSON.stringify(created.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Label could not be created. notCreated: ${notCreated}`, { itemIndex: i })
            }
            returnData.push({
              json: {
                success: true,
                labelId: createdLabelId,
                name: labelName
              },
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'delete') {
            const labelId = this.getNodeParameter('labelId', i) as string
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Mailbox/set', { accountId: mailAccountId, destroy: [labelId] }, 'm1']
            ])
            const result = methodResult<JmapSetResult>(response, 'Mailbox/set')
            returnData.push({
              json: {
                action: 'deleteLabel',
                labelId,
                successful: (result.destroyed ?? []).includes(labelId)
              },
              pairedItem: { item: i }
            })
            continue
          }
        }

        if (resource === 'draft') {
          if (operation === 'create') {
            const identityId = this.getNodeParameter('identityId', i) as string
            const to = parseCsvEmails(this.getNodeParameter('to', i) as string)
            const cc = parseCsvEmails(this.getNodeParameter('cc', i, '') as string)
            const bcc = parseCsvEmails(this.getNodeParameter('bcc', i, '') as string)
            const subject = this.getNodeParameter('subject', i, '') as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string

            const identityResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
              ['Identity/get', { accountId: submissionAccountId, ids: [identityId] }, 'i1']
            ])
            const identity = methodResult<{ list?: IdentityRecord[] }>(identityResponse, 'Identity/get').list?.[0]
            if (identity == null) {
              throw new NodeOperationError(this.getNode(), 'Selected identity was not found', { itemIndex: i })
            }
            const draftMailboxId = await getMailboxIdByRole(this, token, session, mailAccountId, 'drafts')
            if (draftMailboxId == null) {
              throw new NodeOperationError(this.getNode(), 'Drafts mailbox could not be found', { itemIndex: i })
            }

            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to,
              subject,
              keywords: { $draft: true }
            }
            if (cc.length > 0) createEmail.cc = cc
            if (bcc.length > 0) createEmail.bcc = bcc
            if (draftMailboxId) createEmail.mailboxIds = { [draftMailboxId]: true }

            const bodyValues: Record<string, JsonObject> = {}
            if (textBody) {
              bodyValues.textPart = { value: textBody }
              createEmail.textBody = [{ partId: 'textPart', type: 'text/plain' }]
            }
            if (htmlBody) {
              bodyValues.htmlPart = { value: htmlBody }
              createEmail.htmlBody = [{ partId: 'htmlPart', type: 'text/html' }]
            }
            if (Object.keys(bodyValues).length > 0) createEmail.bodyValues = bodyValues

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId: mailAccountId, create: { draft: createEmail } }, 'd1']
            ])

            returnData.push({
              json: {
                success: true,
                draftId: (() => {
                  const emailSetResult = methodResult<JmapSetResult>(response, 'Email/set')
                  const createdId = firstCreatedId(emailSetResult)
                  if (createdId == null) {
                    const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
                    throw new NodeOperationError(this.getNode(), `Draft could not be created. notCreated: ${notCreated}`, { itemIndex: i })
                  }
                  return createdId
                })()
              },
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'get') {
            const draftId = this.getNodeParameter('draftId', i) as string
            const includeBodyValues = this.getNodeParameter('includeBodyValues', i, false) as boolean
            const draft = await getEmailById(this, token, session, mailAccountId, draftId, includeBodyValues)
            if (draft == null) {
              returnData.push({ json: { message: 'Draft not found', draftId }, pairedItem: { item: i } })
            } else {
              returnData.push({ json: simplifyEmail(draft, includeBodyValues), pairedItem: { item: i } })
            }
            continue
          }

          if (operation === 'getMany') {
            const limit = this.getNodeParameter('limit', i, 25)
            const includeBodyValues = this.getNodeParameter('includeBodyValues', i, false) as boolean

            const queryResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/query', { accountId: mailAccountId, filter: { hasKeyword: '$draft' }, limit }, 'q1']
            ])
            const ids = methodResult<{ ids?: string[] }>(queryResponse, 'Email/query').ids ?? []
            const drafts = await getEmailsByIds(this, token, session, mailAccountId, ids, includeBodyValues)

            for (const draft of drafts) {
              returnData.push({ json: simplifyEmail(draft, includeBodyValues), pairedItem: { item: i } })
            }
            continue
          }

          if (operation === 'delete') {
            const draftId = this.getNodeParameter('draftId', i) as string
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId: mailAccountId, destroy: [draftId] }, 'd1']
            ])
            const result = methodResult<JmapSetResult>(response, 'Email/set')
            returnData.push({
              json: {
                action: 'deleteDraft',
                draftId,
                successful: (result.destroyed ?? []).includes(draftId)
              },
              pairedItem: { item: i }
            })
            continue
          }
        }

        if (resource === 'thread') {
          if (operation === 'get') {
            const threadId = this.getNodeParameter('threadId', i) as string
            const includeEmailIds = this.getNodeParameter('includeEmailIds', i, false) as boolean
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Thread/get', { accountId: mailAccountId, ids: [threadId] }, 't1']
            ])
            const thread = methodResult<{ list?: ThreadRecord[] }>(response, 'Thread/get').list?.[0]
            if (thread == null) {
              returnData.push({ json: { message: 'Thread not found', threadId }, pairedItem: { item: i } })
            } else {
              const emails = await getEmailsByIds(this, token, session, mailAccountId, thread.emailIds ?? [])
              const emailMap = new Map(emails.map((email) => [email.id, email]))
              returnData.push({
                json: summarizeThread(thread, emailMap, includeEmailIds),
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'getMany') {
            const limit = this.getNodeParameter('limit', i, 25)
            const filterLabelId = this.getNodeParameter('filterLabelId', i, '') as string
            const includeEmailIds = this.getNodeParameter('includeEmailIds', i, false) as boolean
            const filter = filterLabelId ? { inMailbox: filterLabelId } : {}

            const queryResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/query', { accountId: mailAccountId, filter, limit, collapseThreads: true }, 'eq1']
            ])
            const emailIds = methodResult<{ ids?: string[] }>(queryResponse, 'Email/query').ids ?? []
            const queriedEmails = await getEmailsByIds(this, token, session, mailAccountId, emailIds)
            const threadIds = [...new Set(queriedEmails.map((email) => email.threadId).filter(Boolean))] as string[]
            if (threadIds.length === 0) continue

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Thread/get', { accountId: mailAccountId, ids: threadIds }, 't1']
            ])
            const threads = methodResult<{ list?: ThreadRecord[] }>(response, 'Thread/get').list ?? []
            const allEmailIds = [...new Set(threads.flatMap((thread) => thread.emailIds ?? []))]
            const threadEmails = await getEmailsByIds(this, token, session, mailAccountId, allEmailIds)
            const emailMap = new Map(threadEmails.map((email) => [email.id, email]))

            for (const thread of threads) {
              returnData.push({
                json: summarizeThread(thread, emailMap, includeEmailIds),
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'delete' || operation === 'addLabel' || operation === 'removeLabel' || operation === 'trash' || operation === 'untrash') {
            const threadId = this.getNodeParameter('threadId', i) as string
            const threadEmailIds = await getThreadEmailIds(this, token, session, mailAccountId, threadId)
            if (threadEmailIds.length === 0) {
              returnData.push({ json: { message: 'Thread is empty or not found', threadId }, pairedItem: { item: i } })
              continue
            }

            if (operation === 'delete') {
              const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
                ['Email/set', { accountId: mailAccountId, destroy: threadEmailIds }, 's1']
              ])
              const result = methodResult<JmapSetResult>(response, 'Email/set')
              returnData.push({
                json: {
                  action: 'deleteThread',
                  threadId,
                  requested: threadEmailIds.length,
                  successful: (result.destroyed ?? []).length
                },
                pairedItem: { item: i }
              })
              continue
            }

            let targetLabelId = ''
            if (operation === 'addLabel' || operation === 'removeLabel') {
              targetLabelId = this.getNodeParameter('labelId', i) as string
            }
            if (operation === 'trash' || operation === 'untrash') {
              const trashMailboxId = await getMailboxIdByRole(this, token, session, mailAccountId, 'trash')
              if (trashMailboxId == null) {
                throw new NodeOperationError(this.getNode(), 'Trash mailbox could not be found', { itemIndex: i })
              }
              targetLabelId = trashMailboxId
            }

            const shouldAdd = operation === 'addLabel' || operation === 'trash'
            const update = threadEmailIds.reduce<Record<string, JsonObject>>((acc, emailId) => {
              acc[emailId] = { [`mailboxIds/${targetLabelId}`]: shouldAdd ? true : null }
              return acc
            }, {})

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId: mailAccountId, update }, 's1']
            ])
            const result = methodResult<JmapSetResult>(response, 'Email/set')
            const updatedIds = Object.keys(result.updated ?? {})

            returnData.push({
              json: {
                action: operation,
                threadId,
                requested: threadEmailIds.length,
                successful: updatedIds.length,
                failed: Object.keys(result.notUpdated ?? {}).length
              },
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'reply') {
            const messageId = this.getNodeParameter('replyMessageId', i) as string
            const identityId = this.getNodeParameter('identityId', i) as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string
            const replyAll = this.getNodeParameter('replyAll', i, false) as boolean

            const original = await getEmailById(this, token, session, mailAccountId, messageId)
            if (original == null) {
              throw new NodeOperationError(this.getNode(), 'Original message not found', { itemIndex: i })
            }

            const identityResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
              ['Identity/get', { accountId: submissionAccountId, ids: [identityId] }, 'i1']
            ])
            const identity = methodResult<{ list?: IdentityRecord[] }>(identityResponse, 'Identity/get').list?.[0]
            if (identity == null) {
              throw new NodeOperationError(this.getNode(), 'Selected identity was not found', { itemIndex: i })
            }
            const draftMailboxId = await getMailboxIdByRole(this, token, session, mailAccountId, 'drafts')
            if (draftMailboxId == null) {
              throw new NodeOperationError(this.getNode(), 'Drafts mailbox could not be found', { itemIndex: i })
            }

            const recipients: EmailAddress[] = [...(original.from ?? [])]
            if (replyAll) {
              const addrs = [...(original.to ?? []), ...(original.cc ?? [])]
              for (const addr of addrs) {
                const exists = recipients.some((r) => r.email.toLowerCase() === addr.email.toLowerCase())
                if (!exists && addr.email.toLowerCase() !== identity.email.toLowerCase()) {
                  recipients.push(addr)
                }
              }
            }

            const subject = original.subject?.toLowerCase().startsWith('re:') ? (original.subject ?? '') : `Re: ${original.subject ?? ''}`
            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to: recipients,
              subject,
              keywords: { $draft: true },
              mailboxIds: { [draftMailboxId]: true }
            }

            if (original.messageId?.[0]) {
              createEmail.inReplyTo = [original.messageId[0]]
            }

            const bodyValues: Record<string, JsonObject> = {}
            if (textBody) {
              bodyValues.textPart = { value: textBody }
              createEmail.textBody = [{ partId: 'textPart', type: 'text/plain' }]
            }
            if (htmlBody) {
              bodyValues.htmlPart = { value: htmlBody }
              createEmail.htmlBody = [{ partId: 'htmlPart', type: 'text/html' }]
            }
            if (Object.keys(bodyValues).length > 0) createEmail.bodyValues = bodyValues

            const replyResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION], [
              ['Email/set', { accountId: mailAccountId, create: { replyDraft: createEmail } }, 'c1'],
              ['EmailSubmission/set', { accountId: submissionAccountId, create: { submit: { identityId, emailId: '#replyDraft' } } }, 's1']
            ])

            returnData.push({
              json: {
                success: true,
                replyMessageId: (() => {
                  const emailSetResult = methodResult<JmapSetResult>(replyResponse, 'Email/set')
                  const createdId = firstCreatedId(emailSetResult)
                  if (createdId == null) {
                    const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
                    throw new NodeOperationError(this.getNode(), `Reply could not be created. notCreated: ${notCreated}`, { itemIndex: i })
                  }
                  const submissionResult = methodResult<JmapSetResult>(replyResponse, 'EmailSubmission/set')
                  if (firstCreatedId(submissionResult) == null) {
                    const notCreated = JSON.stringify(submissionResult.notCreated ?? {})
                    throw new NodeOperationError(this.getNode(), `Reply submission failed. notCreated: ${notCreated}`, { itemIndex: i })
                  }
                  return createdId
                })()
              },
              pairedItem: { item: i }
            })
            continue
          }
        }

        throw new NodeOperationError(this.getNode(), `Unsupported combination: ${resource}/${operation}`, { itemIndex: i })
      } catch (error) {
        if (this.continueOnFail()) {
          returnData.push({
            json: {
              error: (error as Error).message
            },
            pairedItem: { item: i }
          })
          continue
        }

        throw error
      }
    }

    return [returnData]
  }
}
