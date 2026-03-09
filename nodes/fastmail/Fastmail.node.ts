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
  sessionState?: string
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
  bodyValues?: Record<string, unknown>
}

interface EmailSetResult {
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

function findMethodResult<T = JsonObject> (response: JmapResponse, methodName: string): T | null {
  const record = response.methodResponses?.find(([name]) => name === methodName)
  if (record == null) return null
  return record[1] as T
}

function parseJsonObject (value: string, label: string, node: IExecuteFunctions, itemIndex: number): JsonObject {
  if (!value.trim()) return {}
  try {
    const parsed = JSON.parse(value) as unknown
    if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
      throw new Error('must be a JSON object')
    }
    return parsed as JsonObject
  } catch (error) {
    throw new NodeOperationError(node.getNode(), `Invalid ${label}: ${(error as Error).message}`, {
      itemIndex
    })
  }
}

function parseCsvEmails (value: string): Array<{ email: string }> {
  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
    .map((email) => ({ email }))
}

function parseCsvIds (value: string): string[] {
  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
}

function formatAddressList (addresses?: EmailAddress[]): string[] {
  return (addresses ?? []).map((entry) => entry.name ? `${entry.name} <${entry.email}>` : entry.email)
}

function simplifyEmail (email: EmailRecord, includeBodyValues: boolean): JsonObject {
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
    isRead: Boolean(email.keywords?.$seen)
  }

  if (includeBodyValues) {
    simplified.bodyValues = email.bodyValues ?? {}
  }

  return simplified
}

function getTokenFromCredentials (credentials: Record<string, unknown>): string {
  const token = credentials.token
  if (typeof token !== 'string' || token.trim() === '') {
    throw new Error('Fastmail API token is missing or invalid in credentials.')
  }
  return token
}

export class Fastmail implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Fastmail',
    name: 'fastmail',
    group: ['transform'],
    version: 1,
    description: 'Work with Fastmail using clear actions and live JMAP-backed dropdowns',
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
        default: 'email',
        options: [
          { name: 'Email', value: 'email' },
          { name: 'Mailbox', value: 'mailbox' },
          { name: 'Identity', value: 'identity' },
          { name: 'Session', value: 'session' },
          { name: 'Raw JMAP (Advanced)', value: 'raw' }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'list',
        displayOptions: {
          show: {
            resource: ['email']
          }
        },
        options: [
          {
            name: 'List in Mailbox',
            value: 'list',
            action: 'List emails in a mailbox'
          },
          {
            name: 'Get by ID',
            value: 'get',
            action: 'Get an email by ID'
          },
          {
            name: 'Mark as Read',
            value: 'markRead',
            action: 'Mark emails as read'
          },
          {
            name: 'Mark as Unread',
            value: 'markUnread',
            action: 'Mark emails as unread'
          },
          {
            name: 'Delete',
            value: 'delete',
            action: 'Delete emails'
          },
          {
            name: 'Send',
            value: 'send',
            action: 'Send an email'
          }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'list',
        displayOptions: {
          show: {
            resource: ['mailbox']
          }
        },
        options: [
          {
            name: 'List',
            value: 'list',
            action: 'List mailboxes'
          },
          {
            name: 'Get by ID',
            value: 'get',
            action: 'Get a mailbox by ID'
          }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'list',
        displayOptions: {
          show: {
            resource: ['identity']
          }
        },
        options: [{
          name: 'List',
          value: 'list',
          action: 'List identities'
        }]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'call',
        displayOptions: {
          show: {
            resource: ['raw']
          }
        },
        options: [{
          name: 'Call',
          value: 'call',
          action: 'Call a custom JMAP method'
        }]
      },
      {
        displayName: 'Mailbox Name or ID',
        name: 'mailboxId',
        type: 'options',
        description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
        typeOptions: {
          loadOptionsMethod: 'getMailboxes'
        },
        required: true,
        default: '',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['list']
          }
        }
      },
      {
        displayName: 'Limit',
        name: 'limit',
        type: 'number',
        description: 'Max number of results to return',
        default: 50,
        typeOptions: {
          minValue: 1
        },
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['list']
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
            resource: ['email'],
            operation: ['list', 'get']
          }
        }
      },
      {
        displayName: 'Email ID',
        name: 'emailId',
        type: 'string',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Email IDs',
        name: 'emailIds',
        type: 'string',
        default: '',
        required: true,
        placeholder: 'id1,id2,id3',
        description: 'Comma-separated email IDs',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['markRead', 'markUnread', 'delete']
          }
        }
      },
      {
        displayName: 'From Identity Name or ID',
        name: 'identityId',
        type: 'options',
        description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
        typeOptions: {
          loadOptionsMethod: 'getIdentities'
        },
        required: true,
        default: '',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['send']
          }
        }
      },
      {
        displayName: 'To',
        name: 'to',
        type: 'string',
        default: '',
        required: true,
        placeholder: 'alice@example.com,bob@example.com',
        description: 'Comma-separated recipient emails',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['send']
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
            resource: ['email'],
            operation: ['send']
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
            resource: ['email'],
            operation: ['send']
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
            resource: ['email'],
            operation: ['send']
          }
        }
      },
      {
        displayName: 'Text Body',
        name: 'textBody',
        type: 'string',
        typeOptions: {
          rows: 6
        },
        default: '',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['send']
          }
        }
      },
      {
        displayName: 'HTML Body',
        name: 'htmlBody',
        type: 'string',
        typeOptions: {
          rows: 6
        },
        default: '',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['send']
          }
        }
      },
      {
        displayName: 'Save To Mailbox Name or ID',
        name: 'saveMailboxId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getMailboxes'
        },
        default: '',
        description: 'Optional mailbox where a copy should be stored. Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['send']
          }
        }
      },
      {
        displayName: 'Mailbox Name or ID',
        name: 'singleMailboxId',
        type: 'options',
        description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
        typeOptions: {
          loadOptionsMethod: 'getMailboxes'
        },
        required: true,
        default: '',
        displayOptions: {
          show: {
            resource: ['mailbox'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Method Name',
        name: 'rawMethodName',
        type: 'string',
        default: 'Mailbox/get',
        displayOptions: {
          show: {
            resource: ['raw']
          }
        }
      },
      {
        displayName: 'Method Arguments (JSON)',
        name: 'rawArgs',
        type: 'string',
        default: '{}',
        displayOptions: {
          show: {
            resource: ['raw']
          }
        }
      },
      {
        displayName: 'Capabilities',
        name: 'rawCapabilities',
        type: 'string',
        default: `${JMAP_MAIL},${JMAP_SUBMISSION}`,
        placeholder: `${JMAP_MAIL},${JMAP_SUBMISSION}`,
        displayOptions: {
          show: {
            resource: ['raw']
          }
        },
        description: 'Comma-separated JMAP capability URNs'
      }
    ]
  }

  methods = {
    loadOptions: {
      async getMailboxes (this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const credentials = (await this.getCredentials('fastmailApi'))
        const token = getTokenFromCredentials(credentials)
        const session = await getSession(this, token)
        const accountId = getPrimaryAccountId(session, JMAP_MAIL)
        const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
          ['Mailbox/get', { accountId, ids: null, properties: ['id', 'name', 'role', 'isSubscribed'] }, 'm1']
        ])

        const list = (methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list ?? [])
          .filter((mailbox) => mailbox.id)
          .map((mailbox) => {
            const roleSuffix = mailbox.role ? ` (${mailbox.role})` : ''
            const label = `${mailbox.name ?? mailbox.id}${roleSuffix}`
            return {
              name: label,
              value: mailbox.id
            }
          })

        return list
      },

      async getIdentities (this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const credentials = (await this.getCredentials('fastmailApi'))
        const token = getTokenFromCredentials(credentials)
        const session = await getSession(this, token)
        const accountId = getPrimaryAccountId(session, JMAP_SUBMISSION)
        const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
          ['Identity/get', { accountId, ids: null, properties: ['id', 'email', 'name'] }, 'i1']
        ])

        const list = (methodResult<{ list?: IdentityRecord[] }>(response, 'Identity/get').list ?? [])
          .filter((identity) => identity.id)
          .map((identity) => ({
            name: identity.name ? `${identity.name} <${identity.email}>` : identity.email,
            value: identity.id
          }))

        return list
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
        const operation = this.getNodeParameter('operation', i, 'list')
        const session = await getSession(this, token)

        if (!session.apiUrl) {
          throw new NodeOperationError(this.getNode(), 'Fastmail API did not return apiUrl', {
            itemIndex: i
          })
        }

        if (resource === 'session') {
          returnData.push({
            json: {
              username: session.username ?? null,
              apiUrl: session.apiUrl,
              primaryAccounts: session.primaryAccounts ?? {}
            },
            pairedItem: { item: i }
          })
          continue
        }

        if (resource === 'mailbox') {
          const accountId = getPrimaryAccountId(session, JMAP_MAIL)
          if (operation === 'list') {
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Mailbox/get', { accountId, ids: null }, 'm1']
            ])

            const mailboxes = methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list ?? []
            if (mailboxes.length === 0) {
              returnData.push({
                json: {
                  message: 'No mailboxes found',
                  count: 0
                },
                pairedItem: { item: i }
              })
              continue
            }

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

          const mailboxId = this.getNodeParameter('singleMailboxId', i) as string
          const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
            ['Mailbox/get', { accountId, ids: [mailboxId] }, 'm1']
          ])
          const mailbox = methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list?.[0]
          if (mailbox == null) {
            returnData.push({
              json: {
                message: 'Mailbox not found',
                mailboxId
              },
              pairedItem: { item: i }
            })
            continue
          }

          returnData.push({
            json: {
              id: mailbox.id,
              name: mailbox.name ?? mailbox.id,
              role: mailbox.role ?? null,
              isSubscribed: mailbox.isSubscribed ?? null
            },
            pairedItem: { item: i }
          })
          continue
        }

        if (resource === 'identity') {
          const accountId = getPrimaryAccountId(session, JMAP_SUBMISSION)
          const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
            ['Identity/get', { accountId, ids: null }, 'i1']
          ])
          const identities = methodResult<{ list?: IdentityRecord[] }>(response, 'Identity/get').list ?? []
          if (identities.length === 0) {
            returnData.push({
              json: {
                message: 'No identities found',
                count: 0
              },
              pairedItem: { item: i }
            })
            continue
          }

          for (const identity of identities) {
            returnData.push({
              json: {
                id: identity.id,
                name: identity.name ?? null,
                email: identity.email
              },
              pairedItem: { item: i }
            })
          }
          continue
        }

        if (resource === 'email') {
          const accountId = getPrimaryAccountId(session, JMAP_MAIL)

          if (operation === 'list') {
            const mailboxId = this.getNodeParameter('mailboxId', i) as string
            const limit = this.getNodeParameter('limit', i, 25)
            const includeBodyValues = this.getNodeParameter('includeBodyValues', i, false) as boolean

            const queryResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              [
                'Email/query',
                {
                  accountId,
                  filter: { inMailbox: mailboxId },
                  sort: [{ property: 'receivedAt', isAscending: false }],
                  position: 0,
                  limit
                },
                'q1'
              ]
            ])
            const queryResult = methodResult<{ ids?: string[] }>(queryResponse, 'Email/query')
            const ids = queryResult.ids ?? []

            if (ids.length === 0) {
              returnData.push({
                json: { message: 'No emails found in mailbox', mailboxId, count: 0 },
                pairedItem: { item: i }
              })
              continue
            }

            const getResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              [
                'Email/get',
                {
                  accountId,
                  ids,
                  properties: [
                    'id',
                    'threadId',
                    'mailboxIds',
                    'from',
                    'to',
                    'cc',
                    'subject',
                    'receivedAt',
                    'keywords',
                    'preview'
                  ],
                  fetchTextBodyValues: includeBodyValues,
                  fetchHTMLBodyValues: includeBodyValues,
                  fetchAllBodyValues: includeBodyValues
                },
                'g1'
              ]
            ])
            const emails = methodResult<{ list?: EmailRecord[] }>(getResponse, 'Email/get').list ?? []
            for (const email of emails) {
              returnData.push({
                json: simplifyEmail(email, includeBodyValues),
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'get') {
            const emailId = this.getNodeParameter('emailId', i) as string
            const includeBodyValues = this.getNodeParameter('includeBodyValues', i, false) as boolean
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              [
                'Email/get',
                {
                  accountId,
                  ids: [emailId],
                  properties: [
                    'id',
                    'threadId',
                    'mailboxIds',
                    'from',
                    'to',
                    'cc',
                    'subject',
                    'receivedAt',
                    'keywords',
                    'preview'
                  ],
                  fetchTextBodyValues: includeBodyValues,
                  fetchHTMLBodyValues: includeBodyValues,
                  fetchAllBodyValues: includeBodyValues
                },
                'g1'
              ]
            ])
            const email = methodResult<{ list?: EmailRecord[] }>(response, 'Email/get').list?.[0]
            if (email == null) {
              returnData.push({
                json: {
                  message: 'Email not found',
                  emailId
                },
                pairedItem: { item: i }
              })
              continue
            }

            returnData.push({
              json: simplifyEmail(email, includeBodyValues),
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'markRead' || operation === 'markUnread') {
            const emailIds = parseCsvIds(this.getNodeParameter('emailIds', i) as string)
            if (emailIds.length === 0) {
              throw new NodeOperationError(this.getNode(), 'Email IDs must not be empty', {
                itemIndex: i
              })
            }
            const isSeen = operation === 'markRead'
            const update = emailIds.reduce<Record<string, JsonObject>>((acc, id) => {
              acc[id] = { 'keywords/$seen': isSeen }
              return acc
            }, {})
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId, update }, 's1']
            ])
            const result = methodResult<EmailSetResult>(response, 'Email/set')
            const updatedIds = Object.keys(result.updated ?? {})
            const failedIds = Object.keys(result.notUpdated ?? {})
            returnData.push({
              json: {
                action: operation === 'markRead' ? 'markedAsRead' : 'markedAsUnread',
                requested: emailIds.length,
                successful: updatedIds.length,
                failed: failedIds.length,
                successfulIds: updatedIds,
                failedIds
              },
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'delete') {
            const emailIds = parseCsvIds(this.getNodeParameter('emailIds', i) as string)
            if (emailIds.length === 0) {
              throw new NodeOperationError(this.getNode(), 'Email IDs must not be empty', {
                itemIndex: i
              })
            }
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId, destroy: emailIds }, 's1']
            ])
            const result = methodResult<EmailSetResult>(response, 'Email/set')
            const destroyedIds = result.destroyed ?? []
            const failedIds = Object.keys(result.notDestroyed ?? {})
            returnData.push({
              json: {
                action: 'deleted',
                requested: emailIds.length,
                successful: destroyedIds.length,
                failed: failedIds.length,
                successfulIds: destroyedIds,
                failedIds
              },
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'send') {
            const submissionAccountId = getPrimaryAccountId(session, JMAP_SUBMISSION)
            const identityId = this.getNodeParameter('identityId', i) as string
            const to = parseCsvEmails(this.getNodeParameter('to', i) as string)
            const cc = parseCsvEmails(this.getNodeParameter('cc', i, '') as string)
            const bcc = parseCsvEmails(this.getNodeParameter('bcc', i, '') as string)
            const subject = this.getNodeParameter('subject', i, '') as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string
            const saveMailboxId = this.getNodeParameter('saveMailboxId', i, '') as string

            if (to.length === 0) {
              throw new NodeOperationError(this.getNode(), 'At least one recipient is required', {
                itemIndex: i
              })
            }

            const identityResponse = await callJmap(
              this,
              token,
              session,
              [JMAP_CORE, JMAP_SUBMISSION],
              [['Identity/get', { accountId: submissionAccountId, ids: [identityId] }, 'i1']]
            )
            const identity = methodResult<{ list?: IdentityRecord[] }>(identityResponse, 'Identity/get').list?.[0]
            if (identity == null) {
              throw new NodeOperationError(this.getNode(), 'Selected identity was not found', {
                itemIndex: i
              })
            }

            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to,
              subject
            }

            if (cc.length > 0) createEmail.cc = cc
            if (bcc.length > 0) createEmail.bcc = bcc
            if (saveMailboxId) createEmail.mailboxIds = { [saveMailboxId]: true }

            const bodyValues: Record<string, JsonObject> = {}
            if (textBody) {
              bodyValues.textPart = { value: textBody }
              createEmail.textBody = [{ partId: 'textPart', type: 'text/plain' }]
            }
            if (htmlBody) {
              bodyValues.htmlPart = { value: htmlBody }
              createEmail.htmlBody = [{ partId: 'htmlPart', type: 'text/html' }]
            }
            if (Object.keys(bodyValues).length > 0) {
              createEmail.bodyValues = bodyValues
            }

            const response = await callJmap(
              this,
              token,
              session,
              [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION],
              [
                ['Email/set', { accountId, create: { draft: createEmail } }, 'c1'],
                [
                  'EmailSubmission/set',
                  {
                    accountId: submissionAccountId,
                    create: { submit: { identityId, emailId: '#draft' } }
                  },
                  's1'
                ]
              ]
            )

            returnData.push({
              json: {
                success: true,
                draftResult: findMethodResult(response, 'Email/set'),
                submissionResult: findMethodResult(response, 'EmailSubmission/set')
              },
              pairedItem: { item: i }
            })
            continue
          }
        }

        if (resource === 'raw') {
          const methodName = this.getNodeParameter('rawMethodName', i) as string
          const rawArgs = this.getNodeParameter('rawArgs', i, '{}') as string
          const rawCapabilities = this.getNodeParameter('rawCapabilities', i, '') as string
          const args = parseJsonObject(rawArgs, 'Method Arguments', this, i)
          const using = [
            JMAP_CORE,
            ...rawCapabilities
              .split(',')
              .map((capability) => capability.trim())
              .filter(Boolean)
          ]
          const uniqueUsing = [...new Set(using)]

          const response = await callJmap(this, token, session, uniqueUsing, [
            [methodName, args, 'r1']
          ])
          returnData.push({
            json: {
              resource,
              operation,
              using: uniqueUsing,
              methodResponses: response.methodResponses ?? [],
              sessionState: response.sessionState ?? null
            },
            pairedItem: { item: i }
          })
          continue
        }

        throw new NodeOperationError(this.getNode(), `Unsupported combination: ${resource}/${operation}`, {
          itemIndex: i
        })
      } catch (error) {
        if (this.continueOnFail()) {
          returnData.push({
            json: {
              error: (error as Error).message
            },
            pairedItem: {
              item: i
            }
          })
          continue
        }

        throw error
      }
    }

    return [returnData]
  }
}
