import {
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeOperationError
} from 'n8n-workflow'

type JsonObject = Record<string, unknown>

type JmapMethodCall = [string, JsonObject, string]

interface SessionResponse {
  apiUrl: string
  username?: string
  accounts?: Record<string, { accountCapabilities?: Record<string, unknown> }>
  primaryAccounts?: Record<string, string>
  capabilities?: Record<string, unknown>
}

interface JmapResponse {
  methodResponses?: Array<[string, JsonObject, string]>
  sessionState?: string
}

const JMAP_CORE_CAPABILITY = 'urn:ietf:params:jmap:core'
const JMAP_MAIL_CAPABILITY = 'urn:ietf:params:jmap:mail'
const JMAP_SUBMISSION_CAPABILITY = 'urn:ietf:params:jmap:submission'
const JMAP_MASKED_EMAIL_CAPABILITY = 'https://www.fastmail.com/dev/maskedemail'

const RESOURCE_CAPABILITY_MAP: Record<string, string[]> = {
  identity: [JMAP_SUBMISSION_CAPABILITY],
  mailbox: [JMAP_MAIL_CAPABILITY],
  email: [JMAP_MAIL_CAPABILITY],
  thread: [JMAP_MAIL_CAPABILITY],
  submission: [JMAP_SUBMISSION_CAPABILITY],
  maskedEmail: [JMAP_MASKED_EMAIL_CAPABILITY]
}

const RESOURCE_ACCOUNT_CAPABILITY_MAP: Record<string, string> = {
  identity: JMAP_SUBMISSION_CAPABILITY,
  mailbox: JMAP_MAIL_CAPABILITY,
  email: JMAP_MAIL_CAPABILITY,
  thread: JMAP_MAIL_CAPABILITY,
  submission: JMAP_SUBMISSION_CAPABILITY,
  maskedEmail: JMAP_MASKED_EMAIL_CAPABILITY
}

function parseJsonParameter (
  value: string,
  parameterName: string,
  node: IExecuteFunctions,
  itemIndex: number
): JsonObject {
  if (!value.trim()) return {}

  try {
    const parsed = JSON.parse(value) as unknown
    if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
      throw new Error('Value must be a JSON object.')
    }
    return parsed as JsonObject
  } catch (error) {
    throw new NodeOperationError(
      node.getNode(),
			`Invalid JSON in ${parameterName}: ${(error as Error).message}`,
			{ itemIndex }
    )
  }
}

function parseJsonArrayParameter (
  value: string,
  parameterName: string,
  node: IExecuteFunctions,
  itemIndex: number
): unknown[] {
  if (!value.trim()) return []

  try {
    const parsed = JSON.parse(value) as unknown
    if (!Array.isArray(parsed)) {
      throw new Error('Value must be a JSON array.')
    }
    return parsed
  } catch (error) {
    throw new NodeOperationError(
      node.getNode(),
			`Invalid JSON in ${parameterName}: ${(error as Error).message}`,
			{ itemIndex }
    )
  }
}

function getPrimaryAccountId (session: SessionResponse, capability: string): string {
  const accountId = session.primaryAccounts?.[capability]
  if (accountId) {
    return accountId
  }

  const matchingAccountId = Object.entries(session.accounts ?? {}).find(([, account]) =>
    Boolean(account.accountCapabilities?.[capability])
  )?.[0]
  if (matchingAccountId) {
    return matchingAccountId
  }

  throw new Error(`Fastmail session did not contain an account for capability ${capability}.`)
}

function normalizeCapabilities (resource: string, customCapabilities: string[]): string[] {
  const required = RESOURCE_CAPABILITY_MAP[resource] ?? []
  const all = [JMAP_CORE_CAPABILITY, ...required, ...customCapabilities]
  return [...new Set(all)]
}

function createMethodCalls (
  node: IExecuteFunctions,
  itemIndex: number,
  session: SessionResponse
): { methodCalls: JmapMethodCall[], usingCapabilities: string[], resolvedAccountId?: string } {
  const resource = node.getNodeParameter('resource', itemIndex)

  if (resource === 'session') {
    return {
      methodCalls: [],
      usingCapabilities: [JMAP_CORE_CAPABILITY]
    }
  }

  if (resource === 'raw') {
    const methodName = node.getNodeParameter('rawMethodName', itemIndex) as string
    const argsJson = node.getNodeParameter('rawMethodArgumentsJson', itemIndex, '{}') as string
    const callId = (node.getNodeParameter('rawCallId', itemIndex, 'call1') as string) || 'call1'
    const customCapabilitiesCsv = node.getNodeParameter('rawCapabilities', itemIndex, '') as string
    const customCapabilities = customCapabilitiesCsv
      .split(',')
      .map((capability) => capability.trim())
      .filter(Boolean)
    const methodArguments = parseJsonParameter(argsJson, 'Method Arguments (JSON)', node, itemIndex)
    const usingCapabilities = normalizeCapabilities('raw', customCapabilities)

    return {
      methodCalls: [[methodName, methodArguments, callId]],
      usingCapabilities
    }
  }

  const operation = node.getNodeParameter('operation', itemIndex)
  const capability = RESOURCE_ACCOUNT_CAPABILITY_MAP[resource]
  const resolvedAccountId = getPrimaryAccountId(session, capability)

  if (resource === 'identity' && operation === 'get') {
    const idsJson = node.getNodeParameter('idsJson', itemIndex, '') as string
    const propertiesJson = node.getNodeParameter('propertiesJson', itemIndex, '') as string

    const args: JsonObject = {
      accountId: resolvedAccountId,
      ids: idsJson.trim() ? parseJsonArrayParameter(idsJson, 'IDs (JSON)', node, itemIndex) : null
    }

    if (propertiesJson.trim()) {
      args.properties = parseJsonArrayParameter(propertiesJson, 'Properties (JSON)', node, itemIndex)
    }

    return {
      methodCalls: [['Identity/get', args, 'identityGet']],
      usingCapabilities: normalizeCapabilities(resource, []),
      resolvedAccountId
    }
  }

  if (resource === 'mailbox') {
    if (operation === 'get') {
      const idsJson = node.getNodeParameter('idsJson', itemIndex, '') as string
      const propertiesJson = node.getNodeParameter('propertiesJson', itemIndex, '') as string

      const args: JsonObject = {
        accountId: resolvedAccountId,
        ids: idsJson.trim() ? parseJsonArrayParameter(idsJson, 'IDs (JSON)', node, itemIndex) : null
      }

      if (propertiesJson.trim()) {
        args.properties = parseJsonArrayParameter(
          propertiesJson,
          'Properties (JSON)',
          node,
          itemIndex
        )
      }

      return {
        methodCalls: [['Mailbox/get', args, 'mailboxGet']],
        usingCapabilities: normalizeCapabilities(resource, []),
        resolvedAccountId
      }
    }

    const createJson = node.getNodeParameter('createJson', itemIndex, '') as string
    const updateJson = node.getNodeParameter('updateJson', itemIndex, '') as string
    const destroyJson = node.getNodeParameter('destroyJson', itemIndex, '') as string
    const onSuccessUpdateEmailJson = node.getNodeParameter('onSuccessUpdateEmailJson', itemIndex, '') as string
    const onSuccessDestroyEmailJson = node.getNodeParameter('onSuccessDestroyEmailJson', itemIndex, '') as string

    const args: JsonObject = {
      accountId: resolvedAccountId
    }

    if (createJson.trim()) args.create = parseJsonParameter(createJson, 'Create (JSON)', node, itemIndex)
    if (updateJson.trim()) args.update = parseJsonParameter(updateJson, 'Update (JSON)', node, itemIndex)
    if (destroyJson.trim()) {
      args.destroy = parseJsonArrayParameter(destroyJson, 'Destroy IDs (JSON)', node, itemIndex)
    }
    if (onSuccessUpdateEmailJson.trim()) {
      args.onSuccessUpdateEmail = parseJsonParameter(
        onSuccessUpdateEmailJson,
        'On Success Update Email (JSON)',
        node,
        itemIndex
      )
    }
    if (onSuccessDestroyEmailJson.trim()) {
      args.onSuccessDestroyEmail = parseJsonArrayParameter(
        onSuccessDestroyEmailJson,
        'On Success Destroy Email IDs (JSON)',
        node,
        itemIndex
      )
    }

    return {
      methodCalls: [['Mailbox/set', args, 'mailboxSet']],
      usingCapabilities: normalizeCapabilities(resource, []),
      resolvedAccountId
    }
  }

  if (resource === 'email') {
    if (operation === 'query') {
      const filterJson = node.getNodeParameter('filterJson', itemIndex, '') as string
      const sortJson = node.getNodeParameter('sortJson', itemIndex, '') as string
      const anchor = node.getNodeParameter('anchor', itemIndex, '') as string
      const anchorOffset = node.getNodeParameter('anchorOffset', itemIndex, 0) as number
      const position = node.getNodeParameter('position', itemIndex, 0) as number
      const limit = node.getNodeParameter('limit', itemIndex, 50)
      const calculateTotal = node.getNodeParameter('calculateTotal', itemIndex, false) as boolean

      const args: JsonObject = {
        accountId: resolvedAccountId,
        position,
        limit,
        calculateTotal
      }

      if (filterJson.trim()) args.filter = parseJsonParameter(filterJson, 'Filter (JSON)', node, itemIndex)
      if (sortJson.trim()) args.sort = parseJsonArrayParameter(sortJson, 'Sort (JSON)', node, itemIndex)
      if (anchor.trim()) {
        args.anchor = anchor
        args.anchorOffset = anchorOffset
      }

      return {
        methodCalls: [['Email/query', args, 'emailQuery']],
        usingCapabilities: normalizeCapabilities(resource, []),
        resolvedAccountId
      }
    }

    if (operation === 'get') {
      const idsJson = node.getNodeParameter('idsJson', itemIndex, '') as string
      const propertiesJson = node.getNodeParameter('propertiesJson', itemIndex, '') as string
      const fetchTextBodyValues = node.getNodeParameter('fetchTextBodyValues', itemIndex, false) as boolean
      const fetchHTMLBodyValues = node.getNodeParameter('fetchHTMLBodyValues', itemIndex, false) as boolean
      const fetchAllBodyValues = node.getNodeParameter('fetchAllBodyValues', itemIndex, false) as boolean
      const maxBodyValueBytes = node.getNodeParameter('maxBodyValueBytes', itemIndex, 0) as number

      const args: JsonObject = {
        accountId: resolvedAccountId,
        ids: idsJson.trim() ? parseJsonArrayParameter(idsJson, 'IDs (JSON)', node, itemIndex) : null,
        fetchTextBodyValues,
        fetchHTMLBodyValues,
        fetchAllBodyValues
      }

      if (propertiesJson.trim()) {
        args.properties = parseJsonArrayParameter(
          propertiesJson,
          'Properties (JSON)',
          node,
          itemIndex
        )
      }

      if (maxBodyValueBytes > 0) {
        args.maxBodyValueBytes = maxBodyValueBytes
      }

      return {
        methodCalls: [['Email/get', args, 'emailGet']],
        usingCapabilities: normalizeCapabilities(resource, []),
        resolvedAccountId
      }
    }

    const createJson = node.getNodeParameter('createJson', itemIndex, '') as string
    const updateJson = node.getNodeParameter('updateJson', itemIndex, '') as string
    const destroyJson = node.getNodeParameter('destroyJson', itemIndex, '') as string

    const args: JsonObject = {
      accountId: resolvedAccountId
    }
    if (createJson.trim()) args.create = parseJsonParameter(createJson, 'Create (JSON)', node, itemIndex)
    if (updateJson.trim()) args.update = parseJsonParameter(updateJson, 'Update (JSON)', node, itemIndex)
    if (destroyJson.trim()) {
      args.destroy = parseJsonArrayParameter(destroyJson, 'Destroy IDs (JSON)', node, itemIndex)
    }

    return {
      methodCalls: [['Email/set', args, 'emailSet']],
      usingCapabilities: normalizeCapabilities(resource, []),
      resolvedAccountId
    }
  }

  if (resource === 'thread') {
    if (operation === 'query') {
      const filterJson = node.getNodeParameter('filterJson', itemIndex, '') as string
      const sortJson = node.getNodeParameter('sortJson', itemIndex, '') as string
      const position = node.getNodeParameter('position', itemIndex, 0) as number
      const limit = node.getNodeParameter('limit', itemIndex, 50)
      const calculateTotal = node.getNodeParameter('calculateTotal', itemIndex, false) as boolean

      const args: JsonObject = {
        accountId: resolvedAccountId,
        position,
        limit,
        calculateTotal
      }
      if (filterJson.trim()) args.filter = parseJsonParameter(filterJson, 'Filter (JSON)', node, itemIndex)
      if (sortJson.trim()) args.sort = parseJsonArrayParameter(sortJson, 'Sort (JSON)', node, itemIndex)

      return {
        methodCalls: [['Thread/query', args, 'threadQuery']],
        usingCapabilities: normalizeCapabilities(resource, []),
        resolvedAccountId
      }
    }

    const idsJson = node.getNodeParameter('idsJson', itemIndex, '') as string
    const args: JsonObject = {
      accountId: resolvedAccountId,
      ids: idsJson.trim() ? parseJsonArrayParameter(idsJson, 'IDs (JSON)', node, itemIndex) : null
    }

    return {
      methodCalls: [['Thread/get', args, 'threadGet']],
      usingCapabilities: normalizeCapabilities(resource, []),
      resolvedAccountId
    }
  }

  if (resource === 'submission') {
    const createJson = node.getNodeParameter('createJson', itemIndex, '') as string
    const updateJson = node.getNodeParameter('updateJson', itemIndex, '') as string
    const destroyJson = node.getNodeParameter('destroyJson', itemIndex, '') as string
    const onSuccessUpdateEmailJson = node.getNodeParameter('onSuccessUpdateEmailJson', itemIndex, '') as string
    const onSuccessDestroyEmailJson = node.getNodeParameter('onSuccessDestroyEmailJson', itemIndex, '') as string

    const args: JsonObject = {
      accountId: resolvedAccountId
    }
    if (createJson.trim()) args.create = parseJsonParameter(createJson, 'Create (JSON)', node, itemIndex)
    if (updateJson.trim()) args.update = parseJsonParameter(updateJson, 'Update (JSON)', node, itemIndex)
    if (destroyJson.trim()) {
      args.destroy = parseJsonArrayParameter(destroyJson, 'Destroy IDs (JSON)', node, itemIndex)
    }
    if (onSuccessUpdateEmailJson.trim()) {
      args.onSuccessUpdateEmail = parseJsonParameter(
        onSuccessUpdateEmailJson,
        'On Success Update Email (JSON)',
        node,
        itemIndex
      )
    }
    if (onSuccessDestroyEmailJson.trim()) {
      args.onSuccessDestroyEmail = parseJsonArrayParameter(
        onSuccessDestroyEmailJson,
        'On Success Destroy Email IDs (JSON)',
        node,
        itemIndex
      )
    }

    return {
      methodCalls: [['EmailSubmission/set', args, 'submissionSet']],
      usingCapabilities: normalizeCapabilities(resource, []),
      resolvedAccountId
    }
  }

  if (resource === 'maskedEmail') {
    if (operation === 'get') {
      const idsJson = node.getNodeParameter('idsJson', itemIndex, '') as string
      const propertiesJson = node.getNodeParameter('propertiesJson', itemIndex, '') as string

      const args: JsonObject = {
        accountId: resolvedAccountId,
        ids: idsJson.trim() ? parseJsonArrayParameter(idsJson, 'IDs (JSON)', node, itemIndex) : null
      }
      if (propertiesJson.trim()) {
        args.properties = parseJsonArrayParameter(
          propertiesJson,
          'Properties (JSON)',
          node,
          itemIndex
        )
      }

      return {
        methodCalls: [['MaskedEmail/get', args, 'maskedEmailGet']],
        usingCapabilities: normalizeCapabilities(resource, []),
        resolvedAccountId
      }
    }

    const createJson = node.getNodeParameter('createJson', itemIndex, '') as string
    const updateJson = node.getNodeParameter('updateJson', itemIndex, '') as string
    const destroyJson = node.getNodeParameter('destroyJson', itemIndex, '') as string

    const args: JsonObject = {
      accountId: resolvedAccountId
    }
    if (createJson.trim()) args.create = parseJsonParameter(createJson, 'Create (JSON)', node, itemIndex)
    if (updateJson.trim()) args.update = parseJsonParameter(updateJson, 'Update (JSON)', node, itemIndex)
    if (destroyJson.trim()) {
      args.destroy = parseJsonArrayParameter(destroyJson, 'Destroy IDs (JSON)', node, itemIndex)
    }

    return {
      methodCalls: [['MaskedEmail/set', args, 'maskedEmailSet']],
      usingCapabilities: normalizeCapabilities(resource, []),
      resolvedAccountId
    }
  }

  throw new Error(`Unsupported resource/operation combination: ${resource}/${operation}`)
}

export class Fastmail implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Fastmail',
    name: 'fastmail',
    group: ['transform'],
    version: 1,
    description: 'Run Fastmail JMAP methods (mail, identities, submission, masked email and raw calls)',
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
        default: 'session',
        options: [
          { name: 'Session', value: 'session' },
          { name: 'Identity', value: 'identity' },
          { name: 'Mailbox', value: 'mailbox' },
          { name: 'Email', value: 'email' },
          { name: 'Thread', value: 'thread' },
          { name: 'Submission', value: 'submission' },
          { name: 'Masked Email', value: 'maskedEmail' },
          { name: 'Raw JMAP', value: 'raw' }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'get',
        displayOptions: {
          show: {
            resource: ['identity']
          }
        },
        options: [{
          name: 'Get',
          value: 'get',
          action: 'Get an identity'
        }]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'get',
        displayOptions: {
          show: {
            resource: ['mailbox']
          }
        },
        options: [
          {
            name: 'Get',
            value: 'get',
            action: 'Get a mailbox'
          },
          {
            name: 'Set',
            value: 'set',
            action: 'Set a mailbox'
          }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'query',
        displayOptions: {
          show: {
            resource: ['email']
          }
        },
        options: [
          {
            name: 'Query',
            value: 'query',
            action: 'Query an email'
          },
          {
            name: 'Get',
            value: 'get',
            action: 'Get an email'
          },
          {
            name: 'Set',
            value: 'set',
            action: 'Set an email'
          }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'query',
        displayOptions: {
          show: {
            resource: ['thread']
          }
        },
        options: [
          {
            name: 'Query',
            value: 'query',
            action: 'Query a thread'
          },
          {
            name: 'Get',
            value: 'get',
            action: 'Get a thread'
          }
        ]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'set',
        displayOptions: {
          show: {
            resource: ['submission']
          }
        },
        options: [{
          name: 'Set',
          value: 'set',
          action: 'Set a submission'
        }]
      },
      {
        displayName: 'Operation',
        name: 'operation',
        type: 'options',
        noDataExpression: true,
        default: 'get',
        displayOptions: {
          show: {
            resource: ['maskedEmail']
          }
        },
        options: [
          {
            name: 'Get',
            value: 'get',
            action: 'Get a masked email'
          },
          {
            name: 'Set',
            value: 'set',
            action: 'Set a masked email'
          }
        ]
      },
      {
        displayName: 'IDs (JSON)',
        name: 'idsJson',
        type: 'string',
        default: '',
        placeholder: '["ID-1", "id-2"]',
        description: 'Optional JSON array of IDs. Empty means all IDs (null).',
        displayOptions: {
          show: {
            resource: ['identity', 'mailbox', 'email', 'thread', 'maskedEmail'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Properties (JSON)',
        name: 'propertiesJson',
        type: 'string',
        default: '',
        placeholder: '["ID", "name"]',
        description: 'Optional JSON array of fields to return',
        displayOptions: {
          show: {
            resource: ['identity', 'mailbox', 'email', 'maskedEmail'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Filter (JSON)',
        name: 'filterJson',
        type: 'string',
        default: '',
        placeholder: '{"inMailbox":"mailbox-ID"}',
        description: 'Optional JSON object filter for query operations',
        displayOptions: {
          show: {
            resource: ['email', 'thread'],
            operation: ['query']
          }
        }
      },
      {
        displayName: 'Sort (JSON)',
        name: 'sortJson',
        type: 'string',
        default: '',
        placeholder: '[{"property":"receivedAt","isAscending":false}]',
        description: 'Optional JSON array sort for query operations',
        displayOptions: {
          show: {
            resource: ['email', 'thread'],
            operation: ['query']
          }
        }
      },
      {
        displayName: 'Position',
        name: 'position',
        type: 'number',
        default: 0,
        description: 'Start index for query',
        displayOptions: {
          show: {
            resource: ['email', 'thread'],
            operation: ['query']
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
            resource: ['email', 'thread'],
            operation: ['query']
          }
        }
      },
      {
        displayName: 'Calculate Total',
        name: 'calculateTotal',
        type: 'boolean',
        default: false,
        description: 'Whether to ask the server for total count',
        displayOptions: {
          show: {
            resource: ['email', 'thread'],
            operation: ['query']
          }
        }
      },
      {
        displayName: 'Anchor',
        name: 'anchor',
        type: 'string',
        default: '',
        description: 'Optional anchor ID for Email/query',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['query']
          }
        }
      },
      {
        displayName: 'Anchor Offset',
        name: 'anchorOffset',
        type: 'number',
        default: 0,
        description: 'Offset relative to anchor',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['query']
          }
        }
      },
      {
        displayName: 'Fetch Text Body Values',
        name: 'fetchTextBodyValues',
        type: 'boolean',
        default: false,
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Fetch HTML Body Values',
        name: 'fetchHTMLBodyValues',
        type: 'boolean',
        default: false,
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Fetch All Body Values',
        name: 'fetchAllBodyValues',
        type: 'boolean',
        default: false,
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Max Body Value Bytes',
        name: 'maxBodyValueBytes',
        type: 'number',
        default: 0,
        description: '0 disables this limit',
        displayOptions: {
          show: {
            resource: ['email'],
            operation: ['get']
          }
        }
      },
      {
        displayName: 'Create (JSON)',
        name: 'createJson',
        type: 'string',
        default: '',
        placeholder: '{"client-ID":{"name":"My mailbox","parentId":null}}',
        description: 'JSON object for JMAP create',
        displayOptions: {
          show: {
            resource: ['mailbox', 'email', 'submission', 'maskedEmail'],
            operation: ['set']
          }
        }
      },
      {
        displayName: 'Update (JSON)',
        name: 'updateJson',
        type: 'string',
        default: '',
        placeholder: '{"mailbox-ID":{"name":"Renamed"}}',
        description: 'JSON object for JMAP update',
        displayOptions: {
          show: {
            resource: ['mailbox', 'email', 'submission', 'maskedEmail'],
            operation: ['set']
          }
        }
      },
      {
        displayName: 'Destroy IDs (JSON)',
        name: 'destroyJson',
        type: 'string',
        default: '',
        placeholder: '["ID-1","id-2"]',
        description: 'JSON array of IDs to destroy',
        displayOptions: {
          show: {
            resource: ['mailbox', 'email', 'submission', 'maskedEmail'],
            operation: ['set']
          }
        }
      },
      {
        displayName: 'On Success Update Email (JSON)',
        name: 'onSuccessUpdateEmailJson',
        type: 'string',
        default: '',
        placeholder: '{"#email-create-ID":{"keywords/$seen":true}}',
        description: 'Optional JSON object for Email updates on success',
        displayOptions: {
          show: {
            resource: ['mailbox', 'submission'],
            operation: ['set']
          }
        }
      },
      {
        displayName: 'On Success Destroy Email IDs (JSON)',
        name: 'onSuccessDestroyEmailJson',
        type: 'string',
        default: '',
        placeholder: '["email-ID-1"]',
        description: 'Optional JSON array for Email IDs to destroy on success',
        displayOptions: {
          show: {
            resource: ['mailbox', 'submission'],
            operation: ['set']
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
        },
        description: 'Exact JMAP method, e.g. Email/query or VacationResponse/get'
      },
      {
        displayName: 'Method Arguments (JSON)',
        name: 'rawMethodArgumentsJson',
        type: 'string',
        default: '{}',
        displayOptions: {
          show: {
            resource: ['raw']
          }
        },
        description: 'JSON object with JMAP method arguments'
      },
      {
        displayName: 'Call ID',
        name: 'rawCallId',
        type: 'string',
        default: 'call1',
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
        default: '',
        placeholder: 'urn:ietf:params:jmap:mail,urn:ietf:params:jmap:submission',
        displayOptions: {
          show: {
            resource: ['raw']
          }
        },
        description: 'Optional comma-separated capability URNs; core is always included'
      }
    ]
  }

  async execute (this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const items = this.getInputData()
    const returnData: INodeExecutionData[] = []
    const credentials = (await this.getCredentials('fastmailApi'))
    const token = credentials.token

    for (let i = 0; i < items.length; i++) {
      try {
        const session = (await this.helpers.httpRequest({
          method: 'GET',
          url: 'https://api.fastmail.com/jmap/session',
          headers: {
            Authorization: `Bearer ${token}`
          },
          json: true
        })) as SessionResponse

        if (!session.apiUrl) {
          throw new NodeOperationError(this.getNode(), 'Fastmail API did not return an apiUrl.', {
            itemIndex: i
          })
        }

        const { methodCalls, usingCapabilities, resolvedAccountId } = createMethodCalls(this, i, session)
        const resource = this.getNodeParameter('resource', i)

        if (resource === 'session') {
          returnData.push({
            json: {
              session
            },
            pairedItem: { item: i }
          })
          continue
        }

        const jmapResponse = (await this.helpers.httpRequest({
          method: 'POST',
          url: session.apiUrl,
          headers: {
            Authorization: `Bearer ${token}`
          },
          body: {
            using: usingCapabilities,
            methodCalls
          },
          json: true
        })) as JmapResponse

        returnData.push({
          json: {
            resource,
            accountId: resolvedAccountId ?? null,
            using: usingCapabilities,
            methodCalls,
            methodResponses: jmapResponse.methodResponses ?? [],
            sessionState: jmapResponse.sessionState ?? null,
            username: session.username ?? null
          },
          pairedItem: { item: i }
        })
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
