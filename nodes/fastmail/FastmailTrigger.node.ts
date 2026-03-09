import {
  IDataObject,
  ILoadOptionsFunctions,
  INodeExecutionData,
  INodePropertyOptions,
  INodeType,
  INodeTypeDescription,
  ITriggerFunctions,
  ITriggerResponse
} from 'n8n-workflow'

type JsonObject = Record<string, any>
type JmapMethodCall = [string, JsonObject, string]

interface SessionResponse {
  apiUrl: string
  eventSourceUrl?: string
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
  subject?: string
  receivedAt?: string
  keywords?: Record<string, boolean>
  preview?: string
}

type TriggerEventType = 'newEmail' | 'deletedEmail' | 'read' | 'unread' | 'updated'

const JMAP_CORE = 'urn:ietf:params:jmap:core'
const JMAP_MAIL = 'urn:ietf:params:jmap:mail'
const MAX_SSE_BUFFER = 2 * 1024 * 1024

async function getSession (node: ITriggerFunctions | ILoadOptionsFunctions, token: string): Promise<SessionResponse> {
  return (await node.helpers.httpRequest({
    method: 'GET',
    url: 'https://api.fastmail.com/jmap/session',
    headers: {
      Authorization: `Bearer ${token}`
    },
    json: true
  })) as SessionResponse
}

function getPrimaryMailAccountId (session: SessionResponse): string {
  const primary = session.primaryAccounts?.[JMAP_MAIL]
  if (primary) return primary

  const discovered = Object.entries(session.accounts ?? {}).find(([, account]) =>
    Boolean(account.accountCapabilities?.[JMAP_MAIL])
  )?.[0]
  if (discovered) return discovered

  throw new Error(`No account found for capability ${JMAP_MAIL}`)
}

function getTokenFromCredentials (credentials: Record<string, unknown>): string {
  const token = credentials.token
  if (typeof token !== 'string' || token.trim() === '') {
    throw new Error('Fastmail API token is missing or invalid in credentials.')
  }
  return token
}

async function callJmap (
  node: ITriggerFunctions | ILoadOptionsFunctions,
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

function formatAddressList (addresses?: EmailAddress[]): string[] {
  return (addresses ?? []).map((entry) => entry.name ? `${entry.name} <${entry.email}>` : entry.email)
}

function simplifyEmail (email: EmailRecord): JsonObject {
  return {
    messageId: email.id,
    subject: email.subject ?? '',
    preview: email.preview ?? '',
    receivedAt: email.receivedAt ?? null,
    threadId: email.threadId ?? null,
    mailboxIds: Object.keys(email.mailboxIds ?? {}),
    from: formatAddressList(email.from),
    to: formatAddressList(email.to),
    isRead: Boolean(email.keywords?.$seen)
  }
}

function buildEventSourceUrl (
  templateUrl: string,
  types: string,
  closeafter: 'no' | 'state',
  ping: number
): string {
  return templateUrl
    .replace(/\{types\}/g, encodeURIComponent(types))
    .replace(/\{closeafter\}/g, encodeURIComponent(closeafter))
    .replace(/\{ping\}/g, encodeURIComponent(String(ping)))
}

async function getMailboxes (
  node: ILoadOptionsFunctions,
  token: string,
  session: SessionResponse,
  accountId: string
): Promise<MailboxRecord[]> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    ['Mailbox/get', { accountId, ids: null, properties: ['id', 'name', 'role'] }, 'm1']
  ])

  return methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list ?? []
}

export class FastmailTrigger implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Fastmail Trigger',
    name: 'fastmailTrigger',
    icon: 'file:fastmail.svg',
    group: ['trigger'],
    version: 1,
    description: 'Triggers on Fastmail message events using JMAP event stream',
    defaults: {
      name: 'Fastmail Trigger'
    },
    inputs: [],
    outputs: ['main'],
    credentials: [
      {
        name: 'fastmailApi',
        required: true
      }
    ],
    properties: [
      {
        displayName: 'Events',
        name: 'events',
        type: 'multiOptions',
        default: ['newEmail'],
        description: 'Which message events should trigger workflow execution',
        options: [
          { name: 'New Email', value: 'newEmail' },
          { name: 'Email Deleted', value: 'deletedEmail' },
          { name: 'Marked as Read', value: 'read' },
          { name: 'Marked as Unread', value: 'unread' },
          { name: 'Message Updated', value: 'updated' }
        ]
      },
      {
        displayName: 'Mailbox Scope',
        name: 'mailboxScope',
        type: 'options',
        default: 'all',
        options: [
          { name: 'All Mailboxes', value: 'all' },
          { name: 'Specific Mailbox', value: 'specific' }
        ],
        description: 'Choose whether to watch all mailboxes or only one specific mailbox'
      },
      {
        displayName: 'Filter by Label Name or ID',
        name: 'filterLabelId',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getLabels'
        },
        description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        default: '',
        required: true,
        displayOptions: {
          show: {
            mailboxScope: ['specific']
          }
        }
      },
      {
        displayName: 'Emit Existing Messages on Start',
        name: 'emitExistingOnStart',
        type: 'boolean',
        default: false,
        description: 'Whether to emit recent matching messages when the trigger starts'
      },
      {
        displayName: 'Existing Messages Limit',
        name: 'initialLimit',
        type: 'number',
        default: 10,
        typeOptions: {
          minValue: 1
        },
        displayOptions: {
          show: {
            emitExistingOnStart: [true]
          }
        },
        description: 'How many existing messages to emit when starting'
      },
      {
        displayName: 'Ping Interval (Seconds)',
        name: 'pingSeconds',
        type: 'number',
        default: 300,
        typeOptions: {
          minValue: 0
        },
        description: 'How often the server should send keepalive ping events'
      },
      {
        displayName: 'Max Reconnect Delay (Seconds)',
        name: 'maxReconnectDelaySeconds',
        type: 'number',
        default: 60,
        typeOptions: {
          minValue: 1
        },
        description: 'Maximum backoff delay after disconnects'
      },
      {
        displayName: 'Force Reconnect Every Minutes',
        name: 'forceReconnectMinutes',
        type: 'number',
        default: 0,
        typeOptions: {
          minValue: 0
        },
        description: 'Set to 0 to disable. If greater than 0, reconnect periodically as a safety measure'
      }
    ]
  }

  methods = {
    loadOptions: {
      async getLabels (this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const credentials = await this.getCredentials('fastmailApi')
        const token = getTokenFromCredentials(credentials)
        const session = await getSession(this, token)
        const accountId = getPrimaryMailAccountId(session)
        const mailboxes = await getMailboxes(this, token, session, accountId)

        return mailboxes
          .filter((mailbox) => mailbox.id)
          .map((mailbox) => ({
            name: mailbox.role ? `${mailbox.name ?? mailbox.id} (${mailbox.role})` : (mailbox.name ?? mailbox.id),
            value: mailbox.id
          }))
      }
    }
  }

  async trigger (this: ITriggerFunctions): Promise<ITriggerResponse> {
    const credentials = await this.getCredentials('fastmailApi')
    const token = getTokenFromCredentials(credentials)
    const events = (this.getNodeParameter('events', ['newEmail']) as string[]) as TriggerEventType[]
    const selectedEvents = new Set<TriggerEventType>(events)
    const mailboxScope = this.getNodeParameter('mailboxScope', 'all') as 'all' | 'specific'
    const selectedLabelId = this.getNodeParameter('filterLabelId', '') as string
    const filterLabelId = mailboxScope === 'specific' ? selectedLabelId : ''
    if (mailboxScope === 'specific' && filterLabelId.trim() === '') {
      throw new Error('Mailbox is required when "Specific Mailbox" is selected.')
    }
    const emitExistingOnStart = this.getNodeParameter('emitExistingOnStart', false) as boolean
    const initialLimit = this.getNodeParameter('initialLimit', 10) as number
    const pingSeconds = this.getNodeParameter('pingSeconds', 300) as number
    const maxReconnectDelaySeconds = this.getNodeParameter('maxReconnectDelaySeconds', 60) as number
    const forceReconnectMinutes = this.getNodeParameter('forceReconnectMinutes', 0) as number

    const staticData = this.getWorkflowStaticData('node') as IDataObject

    let stopped = false
    let abortController: AbortController | undefined
    let reconnectTimer: NodeJS.Timeout | undefined
    let forceReconnectTimer: NodeJS.Timeout | undefined
    let reconnectAttempt = 0
    let connecting = false

    const emitEmailEvent = (event: TriggerEventType, email: EmailRecord, source: 'bootstrap' | 'change'): void => {
      if (!selectedEvents.has(event)) return
      const payload = simplifyEmail(email)
      const item: INodeExecutionData = {
        json: {
          event,
          source,
          ...payload
        }
      }
      this.emit([[item]])
    }

    const emitDeletedEvent = (messageId: string): void => {
      if (!selectedEvents.has('deletedEmail')) return
      const item: INodeExecutionData = {
        json: {
          event: 'deletedEmail',
          source: 'change',
          messageId
        }
      }
      this.emit([[item]])
    }

    const emitEmails = (event: TriggerEventType, source: 'bootstrap' | 'change', emails: EmailRecord[]): void => {
      if (emails.length === 0) return
      for (const email of emails) {
        emitEmailEvent(event, email, source)
      }
    }

    const getQueryState = async (session: SessionResponse, accountId: string): Promise<string> => {
      const filter = filterLabelId ? { inMailbox: filterLabelId } : undefined
      const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
        ['Email/query', { accountId, filter, sort: [{ property: 'receivedAt', isAscending: false }], limit: 1 }, 'q1']
      ])
      return methodResult<{ queryState?: string }>(response, 'Email/query').queryState ?? ''
    }

    const getEmailsByIds = async (session: SessionResponse, accountId: string, ids: string[]): Promise<EmailRecord[]> => {
      if (ids.length === 0) return []
      const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
        [
          'Email/get',
          {
            accountId,
            ids,
            properties: ['id', 'threadId', 'mailboxIds', 'from', 'to', 'subject', 'receivedAt', 'keywords', 'preview']
          },
          'e1'
        ]
      ])
      return methodResult<{ list?: EmailRecord[] }>(response, 'Email/get').list ?? []
    }

    const getEmailState = async (session: SessionResponse, accountId: string): Promise<string> => {
      const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
        ['Email/get', { accountId, ids: [], properties: ['id'] }, 's1']
      ])
      return methodResult<{ state?: string }>(response, 'Email/get').state ?? ''
    }

    const syncChanges = async (session: SessionResponse, accountId: string): Promise<void> => {
      const previousState = typeof staticData.lastQueryState === 'string' ? staticData.lastQueryState : ''
      const previousEmailState = typeof staticData.lastEmailState === 'string' ? staticData.lastEmailState : ''
      const seenMap = (staticData.seenByMessageId as Record<string, boolean> | undefined) ?? {}
      staticData.seenByMessageId = seenMap
      const filter = filterLabelId ? { inMailbox: filterLabelId } : undefined

      if (previousState === '') {
        if (emitExistingOnStart) {
          const bootstrapResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
            ['Email/query', { accountId, filter, sort: [{ property: 'receivedAt', isAscending: false }], limit: initialLimit }, 'b1']
          ])
          const bootstrap = methodResult<{ ids?: string[], queryState?: string }>(bootstrapResponse, 'Email/query')
          const ids = bootstrap.ids ?? []
          const emails = await getEmailsByIds(session, accountId, ids)
          emitEmails('newEmail', 'bootstrap', emails)
          for (const email of emails) {
            seenMap[email.id] = Boolean(email.keywords?.$seen)
          }
          staticData.lastQueryState = bootstrap.queryState ?? ''
        } else {
          staticData.lastQueryState = await getQueryState(session, accountId)
        }

        staticData.lastEmailState = await getEmailState(session, accountId)
        return
      }

      const changesResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
        ['Email/queryChanges', { accountId, filter, sinceQueryState: previousState }, 'c1']
      ])

      const changes = methodResult<{
        removed?: string[]
        added?: Array<{ id: string }>
        newQueryState?: string
        cannotCalculateChanges?: boolean
      }>(changesResponse, 'Email/queryChanges')

      if (changes.cannotCalculateChanges === true || changes.newQueryState == null) {
        staticData.lastQueryState = await getQueryState(session, accountId)
        return
      }

      const addedIds = (changes.added ?? []).map((entry) => entry.id).filter(Boolean)
      if (addedIds.length > 0) {
        const emails = await getEmailsByIds(session, accountId, addedIds)
        for (const email of emails) {
          seenMap[email.id] = Boolean(email.keywords?.$seen)
        }
      }

      staticData.lastQueryState = changes.newQueryState

      if (previousEmailState !== '') {
        const emailChangesResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
          ['Email/changes', { accountId, sinceState: previousEmailState }, 'ec1']
        ])

        const emailChanges = methodResult<{
          created?: string[]
          updated?: string[]
          destroyed?: string[]
          newState?: string
          hasMoreChanges?: boolean
        }>(emailChangesResponse, 'Email/changes')

        if (emailChanges.newState != null) {
          staticData.lastEmailState = emailChanges.newState
        }

        for (const destroyedId of (emailChanges.destroyed ?? [])) {
          delete seenMap[destroyedId]
          emitDeletedEvent(destroyedId)
        }

        const createdIds = [...new Set(emailChanges.created ?? [])]
        const updatedIds = [...new Set(emailChanges.updated ?? [])]
        const changedIds = [...new Set([...createdIds, ...updatedIds])]
        if (changedIds.length > 0) {
          const emails = await getEmailsByIds(session, accountId, changedIds)
          const emailById = new Map(emails.map((email) => [email.id, email]))

          for (const createdId of createdIds) {
            const email = emailById.get(createdId)
            if (email == null) continue
            if (filterLabelId && email.mailboxIds?.[filterLabelId] !== true) continue

            emitEmailEvent('newEmail', email, 'change')
            seenMap[email.id] = Boolean(email.keywords?.$seen)
          }

          for (const updatedId of updatedIds) {
            const email = emailById.get(updatedId)
            if (email == null) continue
            if (filterLabelId && email.mailboxIds?.[filterLabelId] !== true) continue

            const currentSeen = Boolean(email.keywords?.$seen)
            const previousSeen = seenMap[email.id]
            if (previousSeen !== undefined && previousSeen !== currentSeen) {
              emitEmailEvent(currentSeen ? 'read' : 'unread', email, 'change')
            } else if (selectedEvents.has('updated')) {
              emitEmailEvent('updated', email, 'change')
            }

            seenMap[email.id] = currentSeen
          }
        }
      } else {
        staticData.lastEmailState = await getEmailState(session, accountId)
      }
    }

    const resetForceReconnectTimer = (): void => {
      if (forceReconnectTimer != null) clearTimeout(forceReconnectTimer)
      if (forceReconnectMinutes <= 0) return

      forceReconnectTimer = setTimeout(() => {
        if (stopped) return
        if (abortController != null) abortController.abort()
      }, Math.max(1, forceReconnectMinutes) * 60_000)
    }

    const scheduleReconnect = (): void => {
      if (stopped) return
      if (reconnectTimer != null) clearTimeout(reconnectTimer)

      const delayMs = Math.min(maxReconnectDelaySeconds, 2 ** reconnectAttempt) * 1000
      reconnectAttempt += 1
      reconnectTimer = setTimeout(() => {
        void connect()
      }, delayMs)
    }

    const processSseStream = async (
      response: Response,
      onEvent: (eventName: string, data: string, eventId?: string) => Promise<void>
    ): Promise<void> => {
      if (response.body == null) throw new Error('SSE response has no body')

      const reader = response.body.getReader()
      const decoder = new TextDecoder('utf-8')
      let buffer = ''
      let eventName = 'message'
      let eventDataLines: string[] = []
      let eventId: string | undefined

      const dispatchEvent = async (): Promise<void> => {
        if (eventDataLines.length === 0) return
        const data = eventDataLines.join('\n')
        await onEvent(eventName, data, eventId)
        eventName = 'message'
        eventDataLines = []
        eventId = undefined
      }

      while (!stopped) {
        const { done, value } = await reader.read()
        if (done) {
          await dispatchEvent()
          break
        }

        buffer += decoder.decode(value, { stream: true })
        if (buffer.length > MAX_SSE_BUFFER) {
          throw new Error('SSE buffer overflow while parsing stream')
        }

        let lineBreakIndex = buffer.indexOf('\n')
        while (lineBreakIndex >= 0) {
          let rawLine = buffer.slice(0, lineBreakIndex)
          if (rawLine.endsWith('\r')) rawLine = rawLine.slice(0, -1)
          buffer = buffer.slice(lineBreakIndex + 1)

          if (rawLine === '') {
            await dispatchEvent()
          } else if (rawLine.startsWith(':')) {
            // Comment line, ignored.
          } else {
            const separatorIndex = rawLine.indexOf(':')
            const field = separatorIndex >= 0 ? rawLine.slice(0, separatorIndex) : rawLine
            let valuePart = separatorIndex >= 0 ? rawLine.slice(separatorIndex + 1) : ''
            if (valuePart.startsWith(' ')) valuePart = valuePart.slice(1)

            if (field === 'event') eventName = valuePart || 'message'
            if (field === 'data') eventDataLines.push(valuePart)
            if (field === 'id') eventId = valuePart
          }

          lineBreakIndex = buffer.indexOf('\n')
        }
      }
    }

    const connect = async (): Promise<void> => {
      if (stopped || connecting) return
      connecting = true

      try {
        const session = await getSession(this, token)
        const accountId = getPrimaryMailAccountId(session)

        await syncChanges(session, accountId)

        if (session.eventSourceUrl == null || session.eventSourceUrl === '') {
          throw new Error('Fastmail session did not provide an eventSourceUrl')
        }

        const sseUrl = buildEventSourceUrl(session.eventSourceUrl, 'Email', 'no', Math.max(0, pingSeconds))
        abortController = new AbortController()
        resetForceReconnectTimer()

        const headers: Record<string, string> = {
          Authorization: `Bearer ${token}`,
          Accept: 'text/event-stream',
          'Cache-Control': 'no-cache'
        }

        const lastEventId = typeof staticData.lastEventId === 'string' ? staticData.lastEventId : ''
        if (lastEventId !== '') headers['Last-Event-ID'] = lastEventId

        const response = await fetch(sseUrl, {
          method: 'GET',
          headers,
          signal: abortController.signal
        })

        if (!response.ok) {
          throw new Error(`SSE connection failed with HTTP ${response.status}`)
        }

        reconnectAttempt = 0

        await processSseStream(response, async (eventName, eventData, eventId) => {
          if (eventId != null && eventId !== '') {
            staticData.lastEventId = eventId
          }

          if (eventName !== 'state' && eventName !== 'message') {
            return
          }

          if (eventData.trim() === '') {
            return
          }

          await syncChanges(session, accountId)
        })

        if (!stopped) scheduleReconnect()
      } catch (error) {
        if (!stopped) {
          scheduleReconnect()
        }
      } finally {
        connecting = false
      }
    }

    void connect()

    return {
      closeFunction: async () => {
        stopped = true
        if (reconnectTimer != null) clearTimeout(reconnectTimer)
        if (forceReconnectTimer != null) clearTimeout(forceReconnectTimer)
        if (abortController != null) abortController.abort()
      }
    }
  }
}
