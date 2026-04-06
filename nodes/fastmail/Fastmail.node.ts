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
  uploadUrl?: string
  downloadUrl?: string
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
  parentId?: string | null
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
  attachments?: EmailAttachmentPart[]
}

interface EmailAttachmentPart {
  partId?: string
  blobId?: string
  name?: string
  type?: string
  size?: number
  cid?: string
  disposition?: string
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
const ENABLE_FASTMAIL_OAUTH = false
const DEFAULT_FASTMAIL_AUTH_MODE = 'apiToken' as const

type FastmailAuthMode = 'apiToken' | 'oAuth2'

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

function methodResultByCallId<T = JsonObject> (
  response: JmapResponse,
  callId: string,
  expectedMethodName: string
): T {
  const record = response.methodResponses?.find(([, , id]) => id === callId)
  if (record == null) {
    throw new Error(`Missing method response for callId ${callId}`)
  }

  const [methodName, payload] = record
  if (methodName === 'error') {
    throw new Error(`JMAP call ${callId} failed: ${JSON.stringify(payload)}`)
  }

  if (methodName !== expectedMethodName) {
    throw new Error(`Unexpected method response for ${callId}: ${methodName} (expected ${expectedMethodName})`)
  }

  return payload as T
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

function getCredentialTypeForAuthMode (authentication: FastmailAuthMode): string {
  return authentication === 'oAuth2' ? 'fastmailOAuth2Api' : 'fastmailApi'
}

function getAuthModeFromLoadOptions (node: ILoadOptionsFunctions): FastmailAuthMode {
  if (!ENABLE_FASTMAIL_OAUTH) return DEFAULT_FASTMAIL_AUTH_MODE
  const mode = node.getCurrentNodeParameter('authentication')
  return mode === 'oAuth2' ? 'oAuth2' : DEFAULT_FASTMAIL_AUTH_MODE
}

function getTokenFromCredentials (credentials: Record<string, unknown>, authentication: FastmailAuthMode): string {
  if (authentication === 'oAuth2') {
    const oauthTokenData = credentials.oauthTokenData as Record<string, unknown> | undefined
    const oauthToken = oauthTokenData?.access_token ?? oauthTokenData?.accessToken
    if (typeof oauthToken === 'string' && oauthToken.trim() !== '') {
      return oauthToken
    }
    throw new Error('Fastmail OAuth access token is missing. Reconnect OAuth credential.')
  }

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

  const attachments = (email.attachments ?? [])
    .filter((attachment) => typeof attachment.blobId === 'string' && attachment.blobId !== '')
    .map((attachment) => ({
      partId: attachment.partId ?? null,
      blobId: attachment.blobId ?? null,
      fileName: attachment.name ?? null,
      mimeType: attachment.type ?? null,
      size: attachment.size ?? null,
      disposition: attachment.disposition ?? null,
      cid: attachment.cid ?? null
    }))
  if (attachments.length > 0) {
    simplified.attachments = attachments
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

function getEmailProperties (includeBodyValues: boolean, includeAttachments = false): string[] {
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
  if (includeAttachments) {
    base.push('attachments')
  }

  return base
}

function parseBinaryPropertyNames (value: string): string[] {
  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter(Boolean)
}

function hasNonEmptyText (value: unknown): boolean {
  return typeof value === 'string' && value.trim() !== ''
}

interface FetchOptions {
  readStatus?: 'any' | 'read' | 'unread'
  search?: string
  includeBodyValues?: boolean
  downloadAttachments?: boolean
  attachmentBinaryPrefix?: string
  includeEmailIds?: boolean
  includeJmapResponse?: boolean
  readBackMessageAfterUpdate?: boolean
}

interface ComposeOptions {
  cc?: string
  bcc?: string
  replyAll?: boolean
  createAsDraft?: boolean
  attachmentBinaryProperties?: string
}

function escapeHtml (value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

function prefixSubject (subject: string | undefined, prefix: string): string {
  const normalizedSubject = subject ?? ''
  return normalizedSubject.toLowerCase().startsWith(`${prefix.toLowerCase()}:`)
    ? normalizedSubject
    : `${prefix}: ${normalizedSubject}`
}

function buildForwardedTextBody (original: EmailRecord): string {
  const bodyValues = original.bodyValues ?? {}
  const originalText = extractBodyValue(original.textBody, bodyValues) ?? original.preview ?? ''
  const lines = [
    '---------- Forwarded message ----------',
    `From: ${formatAddressList(original.from).join(', ')}`,
    `Date: ${original.receivedAt ?? ''}`,
    `Subject: ${original.subject ?? ''}`,
    `To: ${formatAddressList(original.to).join(', ')}`
  ]

  const ccLine = formatAddressList(original.cc).join(', ')
  if (ccLine !== '') {
    lines.push(`Cc: ${ccLine}`)
  }

  lines.push('', originalText)
  return lines.join('\n')
}

function buildForwardedHtmlBody (original: EmailRecord): string {
  const bodyValues = original.bodyValues ?? {}
  const originalText = extractBodyValue(original.textBody, bodyValues) ?? original.preview ?? ''
  const originalHtml = extractBodyValue(original.htmlBody, bodyValues)
  const fromLine = escapeHtml(formatAddressList(original.from).join(', '))
  const toLine = escapeHtml(formatAddressList(original.to).join(', '))
  const ccLine = escapeHtml(formatAddressList(original.cc).join(', '))
  const receivedAt = escapeHtml(original.receivedAt ?? '')
  const subject = escapeHtml(original.subject ?? '')
  const htmlBody = originalHtml && originalHtml.trim() !== ''
    ? originalHtml
    : `<pre>${escapeHtml(originalText)}</pre>`

  return [
    '<div>---------- Forwarded message ----------</div>',
    `<div><strong>From:</strong> ${fromLine}</div>`,
    `<div><strong>Date:</strong> ${receivedAt}</div>`,
    `<div><strong>Subject:</strong> ${subject}</div>`,
    `<div><strong>To:</strong> ${toLine}</div>`,
    ...(ccLine !== '' ? [`<div><strong>Cc:</strong> ${ccLine}</div>`] : []),
    '<br>',
    htmlBody
  ].join('')
}

function mapOriginalAttachmentsForForward (original: EmailRecord): JsonObject[] {
  return (original.attachments ?? [])
    .filter((attachment) => typeof attachment.blobId === 'string' && attachment.blobId !== '')
    .map((attachment) => ({
      blobId: attachment.blobId,
      type: attachment.type ?? 'application/octet-stream',
      name: attachment.name ?? 'attachment',
      disposition: attachment.disposition ?? 'attachment',
      cid: attachment.cid,
      size: attachment.size
    }))
}

function withDebugData (
  json: JsonObject,
  includeJmapResponse: boolean,
  jmapResponse?: JmapResponse,
  extraDebug: JsonObject = {}
): JsonObject {
  if (!includeJmapResponse && Object.keys(extraDebug).length === 0) {
    return json
  }

  return {
    ...json,
    debug: {
      ...(includeJmapResponse && jmapResponse != null ? { jmapResponse } : {}),
      ...extraDebug
    }
  }
}

function validateFetchOptions (
  node: IExecuteFunctions,
  itemIndex: number,
  resource: string,
  operation: string,
  fetchOptions: FetchOptions
): void {
  if (operation === 'get') {
    if (resource !== 'message' && fetchOptions.downloadAttachments === true) {
      throw new NodeOperationError(node.getNode(), 'Download Attachments is only supported for Message Get/Get Many', { itemIndex })
    }
    if (resource !== 'message' && hasNonEmptyText(fetchOptions.attachmentBinaryPrefix)) {
      throw new NodeOperationError(node.getNode(), 'Attachment Binary Property Prefix is only supported for Message Get/Get Many', { itemIndex })
    }
    if (resource !== 'message' && fetchOptions.includeBodyValues === true && resource !== 'draft') {
      throw new NodeOperationError(node.getNode(), 'Include Body Values is only supported for Message or Draft Get/Get Many', { itemIndex })
    }
    if (resource !== 'thread' && fetchOptions.includeEmailIds === true) {
      throw new NodeOperationError(node.getNode(), 'Include Message IDs is only supported for Thread Get/Get Many', { itemIndex })
    }
    if (hasNonEmptyText(fetchOptions.search) || (fetchOptions.readStatus != null && fetchOptions.readStatus !== 'any')) {
      throw new NodeOperationError(node.getNode(), 'Search and Read Status are only supported for Get Many operations', { itemIndex })
    }
  }

  if (operation === 'getMany') {
    if (resource !== 'message' && fetchOptions.downloadAttachments === true) {
      throw new NodeOperationError(node.getNode(), 'Download Attachments is only supported for Message Get/Get Many', { itemIndex })
    }
    if (resource !== 'message' && hasNonEmptyText(fetchOptions.attachmentBinaryPrefix)) {
      throw new NodeOperationError(node.getNode(), 'Attachment Binary Property Prefix is only supported for Message Get/Get Many', { itemIndex })
    }
    if (resource !== 'message' && resource !== 'draft' && fetchOptions.includeBodyValues === true) {
      throw new NodeOperationError(node.getNode(), 'Include Body Values is only supported for Message or Draft Get/Get Many', { itemIndex })
    }
    if (resource !== 'thread' && fetchOptions.includeEmailIds === true) {
      throw new NodeOperationError(node.getNode(), 'Include Message IDs is only supported for Thread Get/Get Many', { itemIndex })
    }
  }
}

function validateComposeOptions (
  node: IExecuteFunctions,
  itemIndex: number,
  resource: string,
  operation: string,
  composeOptions: ComposeOptions,
  autoFillFromOriginal = true
): void {
  const hasCc = hasNonEmptyText(composeOptions.cc)
  const hasBcc = hasNonEmptyText(composeOptions.bcc)
  const replyAll = composeOptions.replyAll === true
  const createAsDraft = composeOptions.createAsDraft === true
  const allowsManualReplyRecipients = !autoFillFromOriginal && operation === 'reply' && (resource === 'message' || resource === 'thread')

  if (!(resource === 'message' || resource === 'draft' || resource === 'thread') && (hasCc || hasBcc)) {
    throw new NodeOperationError(node.getNode(), 'Cc/Bcc are only supported for Message Send, Message Forward, or Draft Create', { itemIndex })
  }
  if (!(((resource === 'message' || resource === 'draft') && (operation === 'send' || operation === 'create' || operation === 'forward')) || allowsManualReplyRecipients) && (hasCc || hasBcc)) {
    throw new NodeOperationError(node.getNode(), 'Cc/Bcc are only supported for Message Send, Message Forward, Draft Create, or manual Reply', { itemIndex })
  }
  if (!(operation === 'reply' && (resource === 'message' || resource === 'thread') && autoFillFromOriginal) && replyAll) {
    throw new NodeOperationError(node.getNode(), 'Reply All is only supported for Message Reply or Thread Reply when auto-fill is enabled', { itemIndex })
  }
  if (!(((operation === 'reply' && (resource === 'message' || resource === 'thread')) || (operation === 'forward' && resource === 'message'))) && createAsDraft) {
    throw new NodeOperationError(node.getNode(), 'Create as Draft is only supported for Message Reply, Thread Reply, or Message Forward', { itemIndex })
  }
}

function resolveJmapUrlTemplate (template: string, values: Record<string, string>): string {
  return template.replace(/\{([^}]+)\}/g, (_match, key: string) => encodeURIComponent(values[key] ?? ''))
}

function toBinaryPropertyKey (prefix: string, index: number): string {
  const cleanedPrefix = prefix.trim().replace(/[^a-zA-Z0-9_]/g, '_')
  const effectivePrefix = cleanedPrefix === '' ? 'attachment_' : cleanedPrefix
  return `${effectivePrefix}${index + 1}`
}

async function uploadAttachmentsFromBinary (
  node: IExecuteFunctions,
  item: INodeExecutionData,
  itemIndex: number,
  token: string,
  session: SessionResponse,
  accountId: string,
  binaryPropertyNames: string[]
): Promise<{ emailAttachments: JsonObject[], uploadedAttachments: JsonObject[] }> {
  if (binaryPropertyNames.length === 0) {
    return { emailAttachments: [], uploadedAttachments: [] }
  }
  if (typeof session.uploadUrl !== 'string' || session.uploadUrl.trim() === '') {
    throw new NodeOperationError(node.getNode(), 'Fastmail session did not provide an uploadUrl', { itemIndex })
  }

  const emailAttachments: JsonObject[] = []
  const uploadedAttachments: JsonObject[] = []
  const uploadUrl = resolveJmapUrlTemplate(session.uploadUrl, { accountId })

  for (const propertyName of binaryPropertyNames) {
    const binaryData = item.binary?.[propertyName]
    if (binaryData == null) {
      throw new NodeOperationError(node.getNode(), `Binary property "${propertyName}" was not found on input item`, { itemIndex })
    }

    const buffer = await node.helpers.getBinaryDataBuffer(itemIndex, propertyName)
    const fileName = typeof binaryData.fileName === 'string' && binaryData.fileName !== ''
      ? binaryData.fileName
      : propertyName
    const mimeType = typeof binaryData.mimeType === 'string' && binaryData.mimeType !== ''
      ? binaryData.mimeType
      : 'application/octet-stream'

    const uploadResponse = await node.helpers.httpRequest({
      method: 'POST',
      url: uploadUrl,
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': mimeType
      },
      body: buffer,
      json: true
    }) as { blobId?: string, type?: string, size?: number }

    if (typeof uploadResponse.blobId !== 'string' || uploadResponse.blobId === '') {
      throw new NodeOperationError(node.getNode(), `Attachment upload failed for "${propertyName}"`, { itemIndex })
    }

    emailAttachments.push({
      blobId: uploadResponse.blobId,
      type: uploadResponse.type ?? mimeType,
      name: fileName,
      disposition: 'attachment'
    })
    uploadedAttachments.push({
      binaryProperty: propertyName,
      blobId: uploadResponse.blobId,
      fileName,
      mimeType: uploadResponse.type ?? mimeType,
      size: uploadResponse.size ?? null
    })
  }

  return { emailAttachments, uploadedAttachments }
}

async function downloadEmailAttachments (
  node: IExecuteFunctions,
  token: string,
  session: SessionResponse,
  accountId: string,
  email: EmailRecord,
  binaryPropertyPrefix: string
): Promise<{ binary: Record<string, any>, attachments: JsonObject[] }> {
  if (typeof session.downloadUrl !== 'string' || session.downloadUrl.trim() === '') {
    throw new Error('Fastmail session did not provide a downloadUrl')
  }

  const attachments = (email.attachments ?? []).filter((attachment) => typeof attachment.blobId === 'string' && attachment.blobId !== '')
  if (attachments.length === 0) {
    return { binary: {}, attachments: [] }
  }

  const binary: Record<string, any> = {}
  const downloadedAttachments: JsonObject[] = []

  for (let index = 0; index < attachments.length; index++) {
    const attachment = attachments[index]
    const blobId = attachment.blobId as string
    const fileName = attachment.name ?? `attachment-${index + 1}`
    const mimeType = attachment.type ?? 'application/octet-stream'
    const binaryProperty = toBinaryPropertyKey(binaryPropertyPrefix, index)

    const downloadUrl = resolveJmapUrlTemplate(session.downloadUrl, {
      accountId,
      blobId,
      name: fileName,
      type: mimeType
    })

    const rawContent = await node.helpers.httpRequest({
      method: 'GET',
      url: downloadUrl,
      headers: {
        Authorization: `Bearer ${token}`
      },
      encoding: 'arraybuffer'
    }) as Buffer | ArrayBuffer | string

    const contentBuffer = Buffer.isBuffer(rawContent)
      ? rawContent
      : (rawContent instanceof ArrayBuffer)
          ? Buffer.from(rawContent)
          : Buffer.from(rawContent, 'binary')

    binary[binaryProperty] = await node.helpers.prepareBinaryData(contentBuffer, fileName, mimeType)
    downloadedAttachments.push({
      partId: attachment.partId ?? null,
      blobId,
      fileName,
      mimeType,
      size: attachment.size ?? null,
      disposition: attachment.disposition ?? null,
      cid: attachment.cid ?? null,
      binaryProperty
    })
  }

  return { binary, attachments: downloadedAttachments }
}

function buildEmailQueryFilter (
  filterLabelId: string,
  readStatus: 'any' | 'read' | 'unread',
  search: string,
  forceDraftOnly = false
): JsonObject | undefined {
  const conditions: JsonObject[] = []

  if (filterLabelId.trim() !== '') {
    conditions.push({ inMailbox: filterLabelId })
  }

  if (forceDraftOnly) {
    conditions.push({ hasKeyword: '$draft' })
  }

  if (readStatus === 'read') {
    conditions.push({ hasKeyword: '$seen' })
  } else if (readStatus === 'unread') {
    conditions.push({ notKeyword: '$seen' })
  }

  if (search.trim() !== '') {
    conditions.push({ text: search.trim() })
  }

  if (conditions.length === 0) return undefined
  if (conditions.length === 1) return conditions[0]

  return {
    operator: 'AND',
    conditions
  }
}

function hasBodyContent (textBody: string, htmlBody: string): boolean {
  return textBody.trim() !== '' || htmlBody.trim() !== ''
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
    unreadCount: emails.filter((email) => !email.keywords?.$seen).length,
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

function buildMailboxPathMap (mailboxes: MailboxRecord[]): Map<string, string> {
  const byId = new Map(mailboxes.map((mailbox) => [mailbox.id, mailbox]))
  const cache = new Map<string, string>()

  const resolvePath = (mailboxId: string, visiting: Set<string>): string => {
    const cached = cache.get(mailboxId)
    if (cached != null) return cached

    const mailbox = byId.get(mailboxId)
    if (mailbox == null) return mailboxId

    const ownName = mailbox.name?.trim() || mailbox.id
    const parentId = mailbox.parentId ?? ''
    if (parentId === '' || !byId.has(parentId) || visiting.has(mailboxId)) {
      cache.set(mailboxId, ownName)
      return ownName
    }

    visiting.add(mailboxId)
    const parentPath = resolvePath(parentId, visiting)
    visiting.delete(mailboxId)

    const fullPath = `${parentPath} / ${ownName}`
    cache.set(mailboxId, fullPath)
    return fullPath
  }

  for (const mailbox of mailboxes) {
    resolvePath(mailbox.id, new Set<string>())
  }

  return cache
}

async function getMailboxes (
  node: IExecuteFunctions | ILoadOptionsFunctions,
  token: string,
  session: SessionResponse,
  accountId: string
): Promise<MailboxRecord[]> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    ['Mailbox/get', { accountId, ids: null, properties: ['id', 'name', 'role', 'isSubscribed', 'parentId'] }, 'm1']
  ])
  return methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list ?? []
}

async function getIdentities (
  node: IExecuteFunctions | ILoadOptionsFunctions,
  token: string,
  session: SessionResponse,
  accountId: string,
  ids: string[] | null = null
): Promise<IdentityRecord[]> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_SUBMISSION], [
    ['Identity/get', { accountId, ids, properties: ['id', 'email', 'name'] }, 'i1']
  ])

  return methodResult<{ list?: IdentityRecord[] }>(response, 'Identity/get').list ?? []
}

function findReplyIdentity (
  identities: IdentityRecord[],
  original: EmailRecord
): IdentityRecord | null {
  const recipients = [...(original.to ?? []), ...(original.cc ?? [])]
  const recipientEmails = new Set(
    recipients
      .map((entry) => entry.email.trim().toLowerCase())
      .filter((email) => email !== '')
  )

  const matchedIdentity = identities.find((identity) => recipientEmails.has(identity.email.trim().toLowerCase()))
  if (matchedIdentity != null) {
    return matchedIdentity
  }
  return null
}

function assertEmailSetUpdated (
  node: IExecuteFunctions,
  itemIndex: number,
  action: string,
  requestedIds: string[],
  result: JmapSetResult
): string[] {
  const updatedIds = Object.keys(result.updated ?? {})
  const notUpdated = result.notUpdated ?? {}

  if (Object.keys(notUpdated).length > 0 || updatedIds.length !== requestedIds.length) {
    throw new NodeOperationError(
      node.getNode(),
      `${action} failed. updated=${updatedIds.length}/${requestedIds.length}. notUpdated: ${JSON.stringify(notUpdated)}`,
      { itemIndex }
    )
  }

  return updatedIds
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
  includeBodyValues = false,
  includeAttachments = false
): Promise<EmailRecord | null> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    [
      'Email/get',
      {
        accountId,
        ids: [emailId],
        properties: getEmailProperties(includeBodyValues, includeAttachments),
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
  includeBodyValues = false,
  includeAttachments = false
): Promise<EmailRecord[]> {
  if (ids.length === 0) return []

  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    [
      'Email/get',
      {
        accountId,
        ids,
        properties: getEmailProperties(includeBodyValues, includeAttachments),
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

async function destroyEmailById (
  node: IExecuteFunctions,
  token: string,
  session: SessionResponse,
  accountId: string,
  emailId: string
): Promise<void> {
  const response = await callJmap(node, token, session, [JMAP_CORE, JMAP_MAIL], [
    ['Email/set', { accountId, destroy: [emailId] }, 'd1']
  ])

  const result = methodResultByCallId<JmapSetResult>(response, 'd1', 'Email/set')
  if (!(result.destroyed ?? []).includes(emailId)) {
    const notDestroyed = JSON.stringify(result.notDestroyed ?? {})
    throw new Error(`Temporary draft cleanup failed. notDestroyed: ${notDestroyed}`)
  }
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
      ENABLE_FASTMAIL_OAUTH
        ? {
            name: 'fastmailApi',
            required: true,
            displayOptions: {
              show: {
                authentication: ['apiToken']
              }
            }
          }
        : {
            name: 'fastmailApi',
            required: true
          },
      ...(ENABLE_FASTMAIL_OAUTH
        ? [
            {
              name: 'fastmailOAuth2Api',
              required: true,
              displayOptions: {
                show: {
                  authentication: ['oAuth2']
                }
              }
            }
          ]
        : [])
    ],
    properties: [
      ...((ENABLE_FASTMAIL_OAUTH
        ? [
            {
              displayName: 'Authentication',
              name: 'authentication',
              type: 'options',
              options: [
                {
                  name: 'API Token',
                  value: 'apiToken'
                },
                {
                  name: 'OAuth2',
                  value: 'oAuth2'
                }
              ],
              default: DEFAULT_FASTMAIL_AUTH_MODE
            }
          ]
        : []) as INodeTypeDescription['properties']),
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
          { name: 'Forward a Message', value: 'forward', action: 'Forward a message' },
          { name: 'Get a Message', value: 'get', action: 'Get a message' },
          { name: 'Get Many Messages', value: 'getMany', action: 'Get many messages' },
          { name: 'Mark a Message as Not Spam', value: 'markAsNotSpam', action: 'Mark a message as not spam' },
          { name: 'Mark a Message as Phishing', value: 'markAsPhishing', action: 'Mark a message as phishing' },
          { name: 'Mark a Message as Spam', value: 'markAsSpam', action: 'Mark a message as spam' },
          { name: 'Mark a Message as Read', value: 'markRead', action: 'Mark a message as read' },
          { name: 'Mark a Message as Unread', value: 'markUnread', action: 'Mark a message as unread' },
          { name: 'Move a Message', value: 'move', action: 'Move a message to another label' },
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
            operation: ['get', 'delete', 'markRead', 'markUnread', 'markAsSpam', 'markAsNotSpam', 'markAsPhishing', 'addLabel', 'removeLabel', 'move', 'reply', 'forward']
          }
        }
      },
      {
        displayName: 'Label Name or ID',
        name: 'labelIdForMessageOrThread',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getLabels'
        },
        description: 'Choose from the list, or set an ID with an expression. Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['message', 'thread'],
            operation: ['addLabel', 'removeLabel']
          }
        }
      },
      {
        displayName: 'Source Label Name or ID',
        name: 'sourceLabelIdForMove',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getLabels'
        },
        description: 'Mailbox to remove from the message during the move.',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['message'],
            operation: ['move']
          }
        }
      },
      {
        displayName: 'Target Label Name or ID',
        name: 'targetLabelIdForMove',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getLabels'
        },
        description: 'Mailbox to add to the message during the move.',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['message'],
            operation: ['move']
          }
        }
      },
      {
        displayName: 'Label Name or ID',
        name: 'labelIdForLabelResource',
        type: 'options',
        typeOptions: {
          loadOptionsMethod: 'getLabels'
        },
        description: 'Choose from the list, or set an ID with an expression. Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['label'],
            operation: ['get', 'delete']
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
        displayName: 'Mailbox Scope',
        name: 'mailboxScope',
        type: 'options',
        default: 'all',
        options: [
          { name: 'All Mailboxes', value: 'all' },
          { name: 'Specific Mailbox', value: 'specific' }
        ],
        description: 'Choose whether to return results from all mailboxes or only one mailbox',
        displayOptions: {
          show: {
            resource: ['message', 'thread'],
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
        description: 'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>.',
        default: '',
        required: true,
        displayOptions: {
          show: {
            resource: ['message', 'thread'],
            operation: ['getMany'],
            mailboxScope: ['specific']
          }
        }
      },
      {
        displayName: 'Fetch Options',
        name: 'fetchOptions',
        type: 'collection',
        placeholder: 'Add Fetch Option',
        default: {},
        options: [
          {
            displayName: 'Read Status',
            name: 'readStatus',
            type: 'options',
            default: 'any',
            options: [
              { name: 'Any', value: 'any' },
              { name: 'Read', value: 'read' },
              { name: 'Unread', value: 'unread' }
            ]
          },
          {
            displayName: 'Search',
            name: 'search',
            type: 'string',
            default: '',
            placeholder: 'subject, sender, text',
            description: 'Simple full-text search'
          },
          {
            displayName: 'Include Body Values',
            name: 'includeBodyValues',
            type: 'boolean',
            default: false
          },
          {
            displayName: 'Download Attachments',
            name: 'downloadAttachments',
            type: 'boolean',
            default: false,
            description: 'Whether to download message attachments into binary output'
          },
          {
            displayName: 'Attachment Binary Property Prefix',
            name: 'attachmentBinaryPrefix',
            type: 'string',
            default: 'attachment_',
            description: 'Prefix used for binary property names when downloading attachments'
          },
          {
            displayName: 'Include Message IDs',
            name: 'includeEmailIds',
            type: 'boolean',
            default: false
          },
          {
            displayName: 'Debug: Include JMAP Response',
            name: 'includeJmapResponse',
            type: 'boolean',
            default: false,
            description: 'Whether to include raw JMAP response data in the node output when available'
          },
          {
            displayName: 'Debug: Read Back Message After Update',
            name: 'readBackMessageAfterUpdate',
            type: 'boolean',
            default: false,
            description: 'Whether to fetch the message again after supported update operations and include the returned state in debug output'
          }
        ]
      },
      {
        displayName: 'Reply Sender',
        name: 'replyIdentityMode',
        type: 'options',
        default: 'selectedIdentity',
        options: [
          {
            name: 'Use Selected Identity',
            value: 'selectedIdentity'
          },
          {
            name: 'Match Original Recipient',
            value: 'matchOriginalRecipient'
          }
        ],
        description: 'Choose whether replies use the selected identity or the Fastmail identity that originally received the message',
        displayOptions: {
          show: {
            resource: ['message', 'thread'],
            operation: ['reply']
          }
        }
      },
      {
        displayName: 'Auto Fill From Original',
        name: 'autoFillFromOriginal',
        type: 'boolean',
        default: true,
        description: 'Whether to automatically reuse values from the original message',
        displayOptions: {
          show: {
            resource: ['message', 'thread'],
            operation: ['reply']
          }
        }
      },
      {
        displayName: 'Auto Fill From Original',
        name: 'autoFillFromOriginal',
        type: 'boolean',
        default: true,
        description: 'Whether to automatically reuse subject, forwarded content, and original attachments from the original message',
        displayOptions: {
          show: {
            resource: ['message'],
            operation: ['forward']
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
            operation: ['send', 'create', 'forward']
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
            resource: ['message', 'thread'],
            operation: ['reply'],
            replyIdentityMode: ['selectedIdentity']
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
            operation: ['send', 'create', 'forward']
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
            resource: ['message', 'thread'],
            operation: ['reply'],
            autoFillFromOriginal: [false]
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
            operation: ['send', 'create', 'forward']
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
            resource: ['message', 'thread'],
            operation: ['reply'],
            autoFillFromOriginal: [false]
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
            operation: ['send', 'create', 'reply', 'forward']
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
            operation: ['send', 'create', 'reply', 'forward']
          }
        }
      },
      {
        displayName: 'Compose Options',
        name: 'composeOptions',
        type: 'collection',
        placeholder: 'Add Compose Option',
        default: {},
        displayOptions: {
          show: {
            resource: ['message', 'draft', 'thread'],
            operation: ['send', 'create', 'reply', 'forward']
          }
        },
        options: [
          {
            displayName: 'Cc',
            name: 'cc',
            type: 'string',
            default: ''
          },
          {
            displayName: 'Bcc',
            name: 'bcc',
            type: 'string',
            default: ''
          },
          {
            displayName: 'Reply All',
            name: 'replyAll',
            type: 'boolean',
            default: false
          },
          {
            displayName: 'Create as Draft',
            name: 'createAsDraft',
            type: 'boolean',
            default: false,
            description: 'Create the reply or forward as a draft instead of sending it immediately'
          },
          {
            displayName: 'Attachment Binary Properties',
            name: 'attachmentBinaryProperties',
            type: 'string',
            default: '',
            placeholder: 'attachment1,attachment2',
            description: 'Comma-separated binary property names from the input item to upload as attachments'
          }
        ]
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
      }
    ]
  }

  methods = {
    loadOptions: {
      async getLabels (this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const authentication = getAuthModeFromLoadOptions(this)
        const credentialType = getCredentialTypeForAuthMode(authentication)
        const credentials = (await this.getCredentials(credentialType))
        const token = getTokenFromCredentials(credentials, authentication)
        const session = await getSession(this, token)
        const accountId = getPrimaryAccountId(session, JMAP_MAIL)
        const mailboxes = await getMailboxes(this, token, session, accountId)
        const mailboxPathMap = buildMailboxPathMap(mailboxes)

        return mailboxes
          .filter((mailbox) => mailbox.id)
          .map((mailbox) => ({
            name: mailbox.role
              ? `${mailboxPathMap.get(mailbox.id) ?? mailbox.name ?? mailbox.id} (${mailbox.role})`
              : (mailboxPathMap.get(mailbox.id) ?? mailbox.name ?? mailbox.id),
            value: mailbox.id
          }))
      },

      async getIdentities (this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
        const authentication = getAuthModeFromLoadOptions(this)
        const credentialType = getCredentialTypeForAuthMode(authentication)
        const credentials = (await this.getCredentials(credentialType))
        const token = getTokenFromCredentials(credentials, authentication)
        const session = await getSession(this, token)
        const accountId = getPrimaryAccountId(session, JMAP_SUBMISSION)
        const identities = await getIdentities(this, token, session, accountId)
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
    const returnData: INodeExecutionData[] = []

    for (let i = 0; i < items.length; i++) {
      try {
        const resource = this.getNodeParameter('resource', i)
        const operation = this.getNodeParameter('operation', i)
        const authentication = (ENABLE_FASTMAIL_OAUTH
          ? this.getNodeParameter('authentication', i, DEFAULT_FASTMAIL_AUTH_MODE)
          : DEFAULT_FASTMAIL_AUTH_MODE) as FastmailAuthMode
        const credentialType = getCredentialTypeForAuthMode(authentication)
        const credentials = (await this.getCredentials(credentialType))
        const token = getTokenFromCredentials(credentials, authentication)
        const fetchOptions = this.getNodeParameter('fetchOptions', i, {}) as FetchOptions
        const includeJmapResponse = Boolean(fetchOptions.includeJmapResponse ?? false)
        const readBackMessageAfterUpdate = Boolean(fetchOptions.readBackMessageAfterUpdate ?? false)

        const session = await getSession(this, token)
        const mailAccountId = getPrimaryAccountId(session, JMAP_MAIL)
        const submissionAccountId = getPrimaryAccountId(session, JMAP_SUBMISSION)

        if (resource === 'message') {
          if (operation === 'get') {
            validateFetchOptions(this, i, 'message', 'get', fetchOptions)
            const messageId = this.getNodeParameter('messageId', i) as string
            const includeBodyValues = Boolean(fetchOptions.includeBodyValues ?? false)
            const downloadAttachments = Boolean(fetchOptions.downloadAttachments ?? false)
            const attachmentBinaryPrefix = fetchOptions.attachmentBinaryPrefix ?? 'attachment_'
            const email = await getEmailById(this, token, session, mailAccountId, messageId, includeBodyValues, downloadAttachments)
            if (email == null) {
              returnData.push({ json: { message: 'Message not found', messageId }, pairedItem: { item: i } })
            } else {
              const outputItem: INodeExecutionData = {
                json: simplifyEmail(email, includeBodyValues),
                pairedItem: { item: i }
              }
              if (downloadAttachments) {
                const downloaded = await downloadEmailAttachments(this, token, session, mailAccountId, email, attachmentBinaryPrefix)
                if (downloaded.attachments.length > 0) {
                  outputItem.binary = downloaded.binary
                  outputItem.json.attachments = downloaded.attachments
                }
              }
              returnData.push(outputItem)
            }
            continue
          }

          if (operation === 'getMany') {
            validateFetchOptions(this, i, 'message', 'getMany', fetchOptions)
            const limit = this.getNodeParameter('limit', i, 25)
            const mailboxScope = this.getNodeParameter('mailboxScope', i, 'all') as 'all' | 'specific'
            const selectedLabelId = this.getNodeParameter('filterLabelId', i, '') as string
            if (mailboxScope !== 'specific' && selectedLabelId.trim() !== '') {
              throw new NodeOperationError(this.getNode(), 'Filter by Label is only valid when Mailbox Scope is set to "Specific Mailbox"', { itemIndex: i })
            }
            const filterLabelId = mailboxScope === 'specific' ? selectedLabelId : ''
            if (mailboxScope === 'specific' && filterLabelId.trim() === '') {
              throw new NodeOperationError(this.getNode(), 'Mailbox is required when "Specific Mailbox" is selected', { itemIndex: i })
            }
            const readStatus = fetchOptions.readStatus ?? 'any'
            const search = fetchOptions.search ?? ''
            const includeBodyValues = Boolean(fetchOptions.includeBodyValues ?? false)
            const downloadAttachments = Boolean(fetchOptions.downloadAttachments ?? false)
            const attachmentBinaryPrefix = fetchOptions.attachmentBinaryPrefix ?? 'attachment_'

            const filter = buildEmailQueryFilter(filterLabelId, readStatus, search)
            const queryResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/query', { accountId: mailAccountId, filter, sort: [{ property: 'receivedAt', isAscending: false }], limit }, 'q1']
            ])
            const ids = methodResult<{ ids?: string[] }>(queryResponse, 'Email/query').ids ?? []
            const emails = await getEmailsByIds(this, token, session, mailAccountId, ids, includeBodyValues, downloadAttachments)

            for (const email of emails) {
              const outputItem: INodeExecutionData = {
                json: withDebugData(simplifyEmail(email, includeBodyValues), includeJmapResponse, queryResponse),
                pairedItem: { item: i }
              }
              if (downloadAttachments) {
                const downloaded = await downloadEmailAttachments(this, token, session, mailAccountId, email, attachmentBinaryPrefix)
                if (downloaded.attachments.length > 0) {
                  outputItem.binary = downloaded.binary
                  outputItem.json.attachments = downloaded.attachments
                }
              }
              returnData.push(outputItem)
            }
            continue
          }

          if (operation === 'delete' || operation === 'markRead' || operation === 'markUnread' || operation === 'addLabel' || operation === 'removeLabel' || operation === 'move') {
            const messageId = this.getNodeParameter('messageId', i) as string
            const update: JsonObject = {}

            if (operation === 'markRead') update['keywords/$seen'] = true
            if (operation === 'markUnread') update['keywords/$seen'] = false
            if (operation === 'addLabel') {
              const labelId = this.getNodeParameter('labelIdForMessageOrThread', i) as string
              update[`mailboxIds/${labelId}`] = true
            }
            if (operation === 'removeLabel') {
              const labelId = this.getNodeParameter('labelIdForMessageOrThread', i) as string
              update[`mailboxIds/${labelId}`] = null
            }
            if (operation === 'move') {
              const sourceLabelId = this.getNodeParameter('sourceLabelIdForMove', i) as string
              const targetLabelId = this.getNodeParameter('targetLabelIdForMove', i) as string
              if (sourceLabelId === targetLabelId) {
                throw new NodeOperationError(this.getNode(), 'Source Label and Target Label must be different', { itemIndex: i })
              }
              update[`mailboxIds/${sourceLabelId}`] = null
              update[`mailboxIds/${targetLabelId}`] = true
            }

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              operation === 'delete'
                ? ['Email/set', { accountId: mailAccountId, destroy: [messageId] }, 's1']
                : ['Email/set', { accountId: mailAccountId, update: { [messageId]: update } }, 's1']
            ])

            const result = methodResult<JmapSetResult>(response, 'Email/set')
            if (operation !== 'delete') {
              assertEmailSetUpdated(this, i, `Message ${operation}`, [messageId], result)
            }
            let readBackResponse: JmapResponse | undefined
            let readBackEmail: EmailRecord | null = null
            if (operation !== 'delete' && readBackMessageAfterUpdate) {
              readBackResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
                ['Email/get', { accountId: mailAccountId, ids: [messageId], properties: ['id', 'mailboxIds', 'keywords'] }, 'e1']
              ])
              readBackEmail = methodResult<{ list?: EmailRecord[] }>(readBackResponse, 'Email/get').list?.[0] ?? null
            }
            returnData.push({
              json: withDebugData({
                action: operation,
                messageId,
                ...(operation === 'move'
                  ? {
                      sourceLabelId: this.getNodeParameter('sourceLabelIdForMove', i) as string,
                      targetLabelId: this.getNodeParameter('targetLabelIdForMove', i) as string
                    }
                  : {}),
                successful: operation === 'delete' ? (result.destroyed ?? []).includes(messageId) : Object.keys(result.updated ?? {}).includes(messageId)
              }, includeJmapResponse, response, readBackEmail == null
                ? {}
                : {
                    readBackResponse,
                    readBackMessage: {
                      id: readBackEmail.id,
                      mailboxIds: readBackEmail.mailboxIds ?? {},
                      keywords: readBackEmail.keywords ?? {}
                    }
                  }),
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'markAsSpam' || operation === 'markAsNotSpam' || operation === 'markAsPhishing') {
            const messageId = this.getNodeParameter('messageId', i) as string
            const update: JsonObject = {}
            if (operation === 'markAsSpam') {
              update['keywords/$junk'] = true
              update['keywords/$notjunk'] = null
            }
            if (operation === 'markAsNotSpam') {
              update['keywords/$notjunk'] = true
              update['keywords/$junk'] = null
            }
            if (operation === 'markAsPhishing') {
              update['keywords/$phishing'] = true
              update['keywords/$junk'] = true
              update['keywords/$notjunk'] = null
            }

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId: mailAccountId, update: { [messageId]: update } }, 's1']
            ])

            const result = methodResult<JmapSetResult>(response, 'Email/set')
            assertEmailSetUpdated(this, i, `Message ${operation}`, [messageId], result)
            let readBackResponse: JmapResponse | undefined
            let readBackEmail: EmailRecord | null = null
            if (readBackMessageAfterUpdate) {
              readBackResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
                ['Email/get', { accountId: mailAccountId, ids: [messageId], properties: ['id', 'mailboxIds', 'keywords'] }, 'e1']
              ])
              readBackEmail = methodResult<{ list?: EmailRecord[] }>(readBackResponse, 'Email/get').list?.[0] ?? null
            }

            returnData.push({
              json: withDebugData({
                action: operation,
                messageId,
                successful: Object.keys(result.updated ?? {}).includes(messageId),
                updated: result.updated ?? {},
                notUpdated: result.notUpdated ?? {}
              }, includeJmapResponse, response, readBackEmail == null
                ? {}
                : {
                    readBackResponse,
                    readBackMessage: {
                      id: readBackEmail.id,
                      mailboxIds: readBackEmail.mailboxIds ?? {},
                      keywords: readBackEmail.keywords ?? {}
                    }
                  }),
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'send') {
            const composeOptions = this.getNodeParameter('composeOptions', i, {}) as ComposeOptions
            validateComposeOptions(this, i, 'message', 'send', composeOptions)
            const identityId = this.getNodeParameter('identityId', i) as string
            const to = parseCsvEmails(this.getNodeParameter('to', i) as string)
            const cc = parseCsvEmails(composeOptions.cc ?? '')
            const bcc = parseCsvEmails(composeOptions.bcc ?? '')
            const subject = this.getNodeParameter('subject', i, '') as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string
            const attachmentBinaryProperties = parseBinaryPropertyNames(composeOptions.attachmentBinaryProperties ?? '')
            const uploaded = await uploadAttachmentsFromBinary(this, items[i], i, token, session, mailAccountId, attachmentBinaryProperties)
            if (!hasBodyContent(textBody, htmlBody) && uploaded.emailAttachments.length === 0) {
              throw new NodeOperationError(this.getNode(), 'Text Body, HTML Body, or at least one attachment is required', { itemIndex: i })
            }

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
            if (uploaded.emailAttachments.length > 0) createEmail.attachments = uploaded.emailAttachments

            const sendResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION], [
              ['Email/set', { accountId: mailAccountId, create: { draft: createEmail } }, 'c1'],
              [
                'EmailSubmission/set',
                {
                  accountId: submissionAccountId,
                  create: { submit: { identityId, emailId: '#draft' } }
                },
                's1'
              ]
            ])

            const emailSetResult = methodResultByCallId<JmapSetResult>(sendResponse, 'c1', 'Email/set')
            const createdId = firstCreatedId(emailSetResult)
            if (createdId == null) {
              const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Email was not created. notCreated: ${notCreated}`, { itemIndex: i })
            }
            const submissionResult = methodResultByCallId<JmapSetResult>(sendResponse, 's1', 'EmailSubmission/set')
            if (firstCreatedId(submissionResult) == null) {
              const notCreated = JSON.stringify(submissionResult.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Email submission failed. notCreated: ${notCreated}`, { itemIndex: i })
            }

            let draftCleanup: { success: boolean, error?: string } = { success: true }
            try {
              await destroyEmailById(this, token, session, mailAccountId, createdId)
            } catch (error) {
              draftCleanup = { success: false, error: (error as Error).message }
            }

            returnData.push({
              json: withDebugData({
                success: true,
                sentMessageId: createdId,
                draftCleanup,
                uploadedAttachments: uploaded.uploadedAttachments
              }, includeJmapResponse, sendResponse, { identityResponse }),
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'forward') {
            const composeOptions = this.getNodeParameter('composeOptions', i, {}) as ComposeOptions
            const autoFillFromOriginal = Boolean(this.getNodeParameter('autoFillFromOriginal', i, true))
            validateComposeOptions(this, i, 'message', 'forward', composeOptions, autoFillFromOriginal)
            const messageId = this.getNodeParameter('messageId', i) as string
            const identityId = this.getNodeParameter('identityId', i) as string
            const to = parseCsvEmails(this.getNodeParameter('to', i) as string)
            const cc = parseCsvEmails(composeOptions.cc ?? '')
            const bcc = parseCsvEmails(composeOptions.bcc ?? '')
            const subjectInput = this.getNodeParameter('subject', i, '') as string
            const textBodyInput = this.getNodeParameter('textBody', i, '') as string
            const htmlBodyInput = this.getNodeParameter('htmlBody', i, '') as string
            const createAsDraft = Boolean(composeOptions.createAsDraft ?? false)
            const attachmentBinaryProperties = parseBinaryPropertyNames(composeOptions.attachmentBinaryProperties ?? '')
            const uploaded = await uploadAttachmentsFromBinary(this, items[i], i, token, session, mailAccountId, attachmentBinaryProperties)

            if (to.length === 0) {
              throw new NodeOperationError(this.getNode(), 'At least one recipient is required', { itemIndex: i })
            }

            const original = await getEmailById(this, token, session, mailAccountId, messageId, true, true)
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

            const forwardedTextBody = autoFillFromOriginal ? buildForwardedTextBody(original) : ''
            const forwardedHtmlBody = autoFillFromOriginal ? buildForwardedHtmlBody(original) : ''
            const textBody = [textBodyInput, forwardedTextBody].filter((part) => part.trim() !== '').join('\n\n')
            const htmlBody = [htmlBodyInput, forwardedHtmlBody].filter((part) => part.trim() !== '').join('<br><br>')
            const originalAttachments = autoFillFromOriginal ? mapOriginalAttachmentsForForward(original) : []
            const attachments = [...originalAttachments, ...uploaded.emailAttachments]

            if (!hasBodyContent(textBody, htmlBody) && attachments.length === 0) {
              throw new NodeOperationError(this.getNode(), 'Forwarded content could not be created from the original message', { itemIndex: i })
            }

            const subject = subjectInput.trim() !== ''
              ? subjectInput
              : autoFillFromOriginal
                  ? prefixSubject(original.subject, 'Fwd')
                  : ''
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
            if (textBody.trim() !== '') {
              bodyValues.textPart = { value: textBody }
              createEmail.textBody = [{ partId: 'textPart', type: 'text/plain' }]
            }
            if (htmlBody.trim() !== '') {
              bodyValues.htmlPart = { value: htmlBody }
              createEmail.htmlBody = [{ partId: 'htmlPart', type: 'text/html' }]
            }
            if (Object.keys(bodyValues).length > 0) createEmail.bodyValues = bodyValues
            if (attachments.length > 0) createEmail.attachments = attachments

            const forwardResponse = await callJmap(
              this,
              token,
              session,
              createAsDraft ? [JMAP_CORE, JMAP_MAIL] : [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION],
              createAsDraft
                ? [['Email/set', { accountId: mailAccountId, create: { forwardDraft: createEmail } }, 'c1']]
                : [
                    ['Email/set', { accountId: mailAccountId, create: { forwardDraft: createEmail } }, 'c1'],
                    [
                      'EmailSubmission/set',
                      {
                        accountId: submissionAccountId,
                        create: { submit: { identityId, emailId: '#forwardDraft' } }
                      },
                      's1'
                    ]
                  ]
            )

            const emailSetResult = methodResultByCallId<JmapSetResult>(forwardResponse, 'c1', 'Email/set')
            const createdId = firstCreatedId(emailSetResult)
            if (createdId == null) {
              const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Forward could not be created. notCreated: ${notCreated}`, { itemIndex: i })
            }

            if (createAsDraft) {
              returnData.push({
                json: withDebugData({
                  success: true,
                  draftId: createdId,
                  submitted: false,
                  sourceMessageId: messageId,
                  originalAttachmentCount: originalAttachments.length,
                  uploadedAttachments: uploaded.uploadedAttachments
                }, includeJmapResponse, forwardResponse, { identityResponse }),
                pairedItem: { item: i }
              })
            } else {
              const submissionResult = methodResultByCallId<JmapSetResult>(forwardResponse, 's1', 'EmailSubmission/set')
              if (firstCreatedId(submissionResult) == null) {
                const notCreated = JSON.stringify(submissionResult.notCreated ?? {})
                throw new NodeOperationError(this.getNode(), `Forward submission failed. notCreated: ${notCreated}`, { itemIndex: i })
              }

              let draftCleanup: { success: boolean, error?: string } = { success: true }
              try {
                await destroyEmailById(this, token, session, mailAccountId, createdId)
              } catch (error) {
                draftCleanup = { success: false, error: (error as Error).message }
              }

              returnData.push({
                json: withDebugData({
                  success: true,
                  forwardedMessageId: createdId,
                  sourceMessageId: messageId,
                  submitted: true,
                  originalAttachmentCount: originalAttachments.length,
                  draftCleanup,
                  uploadedAttachments: uploaded.uploadedAttachments
                }, includeJmapResponse, forwardResponse, { identityResponse }),
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'reply') {
            const composeOptions = this.getNodeParameter('composeOptions', i, {}) as ComposeOptions
            const autoFillFromOriginal = Boolean(this.getNodeParameter('autoFillFromOriginal', i, true))
            validateComposeOptions(this, i, 'message', 'reply', composeOptions, autoFillFromOriginal)
            const messageId = this.getNodeParameter('messageId', i) as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string
            const attachmentBinaryProperties = parseBinaryPropertyNames(composeOptions.attachmentBinaryProperties ?? '')
            const uploaded = await uploadAttachmentsFromBinary(this, items[i], i, token, session, mailAccountId, attachmentBinaryProperties)
            if (!hasBodyContent(textBody, htmlBody) && uploaded.emailAttachments.length === 0) {
              throw new NodeOperationError(this.getNode(), 'Text Body, HTML Body, or at least one attachment is required', { itemIndex: i })
            }
            const replyAll = Boolean(composeOptions.replyAll ?? false)
            const createAsDraft = Boolean(composeOptions.createAsDraft ?? false)
            const replyIdentityMode = this.getNodeParameter('replyIdentityMode', i, 'selectedIdentity') as string

            const original = await getEmailById(this, token, session, mailAccountId, messageId)
            if (original == null) {
              throw new NodeOperationError(this.getNode(), 'Original message not found', { itemIndex: i })
            }

            const identity = replyIdentityMode === 'matchOriginalRecipient'
              ? findReplyIdentity(await getIdentities(this, token, session, submissionAccountId), original)
              : (await getIdentities(this, token, session, submissionAccountId, [this.getNodeParameter('identityId', i) as string]))[0] ?? null
            if (identity == null) {
              throw new NodeOperationError(
                this.getNode(),
                replyIdentityMode === 'matchOriginalRecipient'
                  ? 'No Fastmail identity matched the original message recipients'
                  : 'Selected identity was not found',
                { itemIndex: i }
              )
            }
            const draftMailboxId = await getMailboxIdByRole(this, token, session, mailAccountId, 'drafts')
            if (draftMailboxId == null) {
              throw new NodeOperationError(this.getNode(), 'Drafts mailbox could not be found', { itemIndex: i })
            }

            const recipients: EmailAddress[] = autoFillFromOriginal
              ? [...(original.from ?? [])]
              : parseCsvEmails(this.getNodeParameter('to', i, '') as string)
            if (recipients.length === 0) {
              throw new NodeOperationError(this.getNode(), 'At least one recipient is required', { itemIndex: i })
            }
            if (autoFillFromOriginal && replyAll) {
              const addrs = [...(original.to ?? []), ...(original.cc ?? [])]
              for (const addr of addrs) {
                const exists = recipients.some((r) => r.email.toLowerCase() === addr.email.toLowerCase())
                if (!exists && addr.email.toLowerCase() !== identity.email.toLowerCase()) {
                  recipients.push(addr)
                }
              }
            }

            const subject = autoFillFromOriginal
              ? prefixSubject(original.subject, 'Re')
              : (this.getNodeParameter('subject', i, '') as string)
            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to: recipients,
              subject,
              keywords: { $draft: true },
              mailboxIds: { [draftMailboxId]: true }
            }
            const cc = parseCsvEmails(composeOptions.cc ?? '')
            const bcc = parseCsvEmails(composeOptions.bcc ?? '')
            if (cc.length > 0) createEmail.cc = cc
            if (bcc.length > 0) createEmail.bcc = bcc

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
            if (uploaded.emailAttachments.length > 0) createEmail.attachments = uploaded.emailAttachments

            const replyResponse = await callJmap(
              this,
              token,
              session,
              createAsDraft ? [JMAP_CORE, JMAP_MAIL] : [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION],
              createAsDraft
                ? [['Email/set', { accountId: mailAccountId, create: { replyDraft: createEmail } }, 'c1']]
                : [
                    ['Email/set', { accountId: mailAccountId, create: { replyDraft: createEmail } }, 'c1'],
                    [
                      'EmailSubmission/set',
                      {
                        accountId: submissionAccountId,
                        create: { submit: { identityId: identity.id, emailId: '#replyDraft' } }
                      },
                      's1'
                    ]
                  ]
            )

            const emailSetResult = methodResultByCallId<JmapSetResult>(replyResponse, 'c1', 'Email/set')
            const createdReplyId = firstCreatedId(emailSetResult)
            if (createdReplyId == null) {
              const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Reply could not be created. notCreated: ${notCreated}`, { itemIndex: i })
            }
            if (createAsDraft) {
              returnData.push({
                json: withDebugData({
                  success: true,
                  draftId: createdReplyId,
                  submitted: false,
                  uploadedAttachments: uploaded.uploadedAttachments
                }, includeJmapResponse, replyResponse),
                pairedItem: { item: i }
              })
            } else {
              const submissionResult = methodResultByCallId<JmapSetResult>(replyResponse, 's1', 'EmailSubmission/set')
              if (firstCreatedId(submissionResult) == null) {
                const notCreated = JSON.stringify(submissionResult.notCreated ?? {})
                throw new NodeOperationError(this.getNode(), `Reply submission failed. notCreated: ${notCreated}`, { itemIndex: i })
              }

              let draftCleanup: { success: boolean, error?: string } = { success: true }
              try {
                await destroyEmailById(this, token, session, mailAccountId, createdReplyId)
              } catch (error) {
                draftCleanup = { success: false, error: (error as Error).message }
              }

              returnData.push({
                json: withDebugData({
                  success: true,
                  replyMessageId: createdReplyId,
                  submitted: true,
                  draftCleanup,
                  uploadedAttachments: uploaded.uploadedAttachments
                }, includeJmapResponse, replyResponse),
                pairedItem: { item: i }
              })
            }
            continue
          }
        }

        if (resource === 'label') {
          if (operation === 'getMany') {
            const mailboxes = await getMailboxes(this, token, session, mailAccountId)
            const mailboxPathMap = buildMailboxPathMap(mailboxes)
            for (const mailbox of mailboxes) {
              returnData.push({
                json: withDebugData({
                  id: mailbox.id,
                  name: mailbox.name ?? mailbox.id,
                  path: mailboxPathMap.get(mailbox.id) ?? mailbox.name ?? mailbox.id,
                  parentId: mailbox.parentId ?? null,
                  role: mailbox.role ?? null,
                  isSubscribed: mailbox.isSubscribed ?? null
                }, includeJmapResponse, undefined, {
                  note: 'Raw JMAP response is not attached for this helper-backed operation.'
                }),
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'get') {
            const labelId = this.getNodeParameter('labelIdForLabelResource', i) as string
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Mailbox/get', { accountId: mailAccountId, ids: [labelId], properties: ['id', 'name', 'role', 'isSubscribed', 'parentId'] }, 'm1']
            ])
            const mailbox = methodResult<{ list?: MailboxRecord[] }>(response, 'Mailbox/get').list?.[0]
            if (mailbox == null) {
              returnData.push({ json: { message: 'Label not found', labelId }, pairedItem: { item: i } })
            } else {
              const allMailboxes = await getMailboxes(this, token, session, mailAccountId)
              const mailboxPathMap = buildMailboxPathMap(allMailboxes)
              returnData.push({
                json: withDebugData({
                  id: mailbox.id,
                  name: mailbox.name ?? mailbox.id,
                  path: mailboxPathMap.get(mailbox.id) ?? mailbox.name ?? mailbox.id,
                  parentId: mailbox.parentId ?? null,
                  role: mailbox.role ?? null,
                  isSubscribed: mailbox.isSubscribed ?? null
                }, includeJmapResponse, response),
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
              json: withDebugData({
                success: true,
                labelId: createdLabelId,
                name: labelName
              }, includeJmapResponse, response),
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'delete') {
            const labelId = this.getNodeParameter('labelIdForLabelResource', i) as string
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Mailbox/set', { accountId: mailAccountId, destroy: [labelId] }, 'm1']
            ])
            const result = methodResult<JmapSetResult>(response, 'Mailbox/set')
            returnData.push({
              json: withDebugData({
                action: 'deleteLabel',
                labelId,
                successful: (result.destroyed ?? []).includes(labelId)
              }, includeJmapResponse, response),
              pairedItem: { item: i }
            })
            continue
          }
        }

        if (resource === 'draft') {
          if (operation === 'create') {
            const composeOptions = this.getNodeParameter('composeOptions', i, {}) as ComposeOptions
            validateComposeOptions(this, i, 'draft', 'create', composeOptions)
            const identityId = this.getNodeParameter('identityId', i) as string
            const to = parseCsvEmails(this.getNodeParameter('to', i) as string)
            const cc = parseCsvEmails(composeOptions.cc ?? '')
            const bcc = parseCsvEmails(composeOptions.bcc ?? '')
            const subject = this.getNodeParameter('subject', i, '') as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string
            const attachmentBinaryProperties = parseBinaryPropertyNames(composeOptions.attachmentBinaryProperties ?? '')
            const uploaded = await uploadAttachmentsFromBinary(this, items[i], i, token, session, mailAccountId, attachmentBinaryProperties)
            if (!hasBodyContent(textBody, htmlBody) && uploaded.emailAttachments.length === 0) {
              throw new NodeOperationError(this.getNode(), 'Text Body, HTML Body, or at least one attachment is required', { itemIndex: i })
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
            if (uploaded.emailAttachments.length > 0) createEmail.attachments = uploaded.emailAttachments

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId: mailAccountId, create: { draft: createEmail } }, 'd1']
            ])

            returnData.push({
              json: withDebugData({
                success: true,
                uploadedAttachments: uploaded.uploadedAttachments,
                draftId: (() => {
                  const emailSetResult = methodResult<JmapSetResult>(response, 'Email/set')
                  const createdId = firstCreatedId(emailSetResult)
                  if (createdId == null) {
                    const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
                    throw new NodeOperationError(this.getNode(), `Draft could not be created. notCreated: ${notCreated}`, { itemIndex: i })
                  }
                  return createdId
                })()
              }, includeJmapResponse, response, { identityResponse }),
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'get') {
            validateFetchOptions(this, i, 'draft', 'get', fetchOptions)
            const draftId = this.getNodeParameter('draftId', i) as string
            const includeBodyValues = Boolean(fetchOptions.includeBodyValues ?? false)
            const draft = await getEmailById(this, token, session, mailAccountId, draftId, includeBodyValues)
            if (draft == null) {
              returnData.push({ json: { message: 'Draft not found', draftId }, pairedItem: { item: i } })
            } else {
              returnData.push({ json: simplifyEmail(draft, includeBodyValues), pairedItem: { item: i } })
            }
            continue
          }

          if (operation === 'getMany') {
            validateFetchOptions(this, i, 'draft', 'getMany', fetchOptions)
            const limit = this.getNodeParameter('limit', i, 25)
            const includeBodyValues = Boolean(fetchOptions.includeBodyValues ?? false)
            const search = fetchOptions.search ?? ''

            const filter = buildEmailQueryFilter('', 'any', search, true)
            const queryResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/query', { accountId: mailAccountId, filter, limit }, 'q1']
            ])
            const ids = methodResult<{ ids?: string[] }>(queryResponse, 'Email/query').ids ?? []
            const drafts = await getEmailsByIds(this, token, session, mailAccountId, ids, includeBodyValues)

            for (const draft of drafts) {
              returnData.push({
                json: withDebugData(simplifyEmail(draft, includeBodyValues), includeJmapResponse, queryResponse),
                pairedItem: { item: i }
              })
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
              json: withDebugData({
                action: 'deleteDraft',
                draftId,
                successful: (result.destroyed ?? []).includes(draftId)
              }, includeJmapResponse, response),
              pairedItem: { item: i }
            })
            continue
          }
        }

        if (resource === 'thread') {
          if (operation === 'get') {
            validateFetchOptions(this, i, 'thread', 'get', fetchOptions)
            const threadId = this.getNodeParameter('threadId', i) as string
            const includeEmailIds = Boolean(fetchOptions.includeEmailIds ?? false)
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
                json: withDebugData(summarizeThread(thread, emailMap, includeEmailIds), includeJmapResponse, response),
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'getMany') {
            validateFetchOptions(this, i, 'thread', 'getMany', fetchOptions)
            const limit = this.getNodeParameter('limit', i, 25)
            const mailboxScope = this.getNodeParameter('mailboxScope', i, 'all') as 'all' | 'specific'
            const selectedLabelId = this.getNodeParameter('filterLabelId', i, '') as string
            if (mailboxScope !== 'specific' && selectedLabelId.trim() !== '') {
              throw new NodeOperationError(this.getNode(), 'Filter by Label is only valid when Mailbox Scope is set to "Specific Mailbox"', { itemIndex: i })
            }
            const filterLabelId = mailboxScope === 'specific' ? selectedLabelId : ''
            if (mailboxScope === 'specific' && filterLabelId.trim() === '') {
              throw new NodeOperationError(this.getNode(), 'Mailbox is required when "Specific Mailbox" is selected', { itemIndex: i })
            }
            const readStatus = fetchOptions.readStatus ?? 'any'
            const search = fetchOptions.search ?? ''
            const includeEmailIds = Boolean(fetchOptions.includeEmailIds ?? false)
            const filter = buildEmailQueryFilter(filterLabelId, readStatus, search)

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
                json: withDebugData(summarizeThread(thread, emailMap, includeEmailIds), includeJmapResponse, response, { emailQueryResponse: queryResponse }),
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
                json: withDebugData({
                  action: 'deleteThread',
                  threadId,
                  requested: threadEmailIds.length,
                  successful: (result.destroyed ?? []).length
                }, includeJmapResponse, response),
                pairedItem: { item: i }
              })
              continue
            }

            let targetLabelId = ''
            if (operation === 'addLabel' || operation === 'removeLabel') {
              targetLabelId = this.getNodeParameter('labelIdForMessageOrThread', i) as string
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
            const updatedIds = assertEmailSetUpdated(this, i, `Thread ${operation}`, threadEmailIds, result)

            returnData.push({
              json: withDebugData({
                action: operation,
                threadId,
                requested: threadEmailIds.length,
                successful: updatedIds.length,
                failed: 0
              }, includeJmapResponse, response),
              pairedItem: { item: i }
            })
            continue
          }

          if (operation === 'reply') {
            const composeOptions = this.getNodeParameter('composeOptions', i, {}) as ComposeOptions
            const autoFillFromOriginal = Boolean(this.getNodeParameter('autoFillFromOriginal', i, true))
            validateComposeOptions(this, i, 'thread', 'reply', composeOptions, autoFillFromOriginal)
            const messageId = this.getNodeParameter('replyMessageId', i) as string
            const textBody = this.getNodeParameter('textBody', i, '') as string
            const htmlBody = this.getNodeParameter('htmlBody', i, '') as string
            const replyAll = Boolean(composeOptions.replyAll ?? false)
            const createAsDraft = Boolean(composeOptions.createAsDraft ?? false)
            const replyIdentityMode = this.getNodeParameter('replyIdentityMode', i, 'selectedIdentity') as string
            const attachmentBinaryProperties = parseBinaryPropertyNames(composeOptions.attachmentBinaryProperties ?? '')
            const uploaded = await uploadAttachmentsFromBinary(this, items[i], i, token, session, mailAccountId, attachmentBinaryProperties)
            if (!hasBodyContent(textBody, htmlBody) && uploaded.emailAttachments.length === 0) {
              throw new NodeOperationError(this.getNode(), 'Text Body, HTML Body, or at least one attachment is required', { itemIndex: i })
            }

            const original = await getEmailById(this, token, session, mailAccountId, messageId)
            if (original == null) {
              throw new NodeOperationError(this.getNode(), 'Original message not found', { itemIndex: i })
            }

            const identity = replyIdentityMode === 'matchOriginalRecipient'
              ? findReplyIdentity(await getIdentities(this, token, session, submissionAccountId), original)
              : (await getIdentities(this, token, session, submissionAccountId, [this.getNodeParameter('identityId', i) as string]))[0] ?? null
            if (identity == null) {
              throw new NodeOperationError(
                this.getNode(),
                replyIdentityMode === 'matchOriginalRecipient'
                  ? 'No Fastmail identity matched the original message recipients'
                  : 'Selected identity was not found',
                { itemIndex: i }
              )
            }
            const draftMailboxId = await getMailboxIdByRole(this, token, session, mailAccountId, 'drafts')
            if (draftMailboxId == null) {
              throw new NodeOperationError(this.getNode(), 'Drafts mailbox could not be found', { itemIndex: i })
            }

            const recipients: EmailAddress[] = autoFillFromOriginal
              ? [...(original.from ?? [])]
              : parseCsvEmails(this.getNodeParameter('to', i, '') as string)
            if (recipients.length === 0) {
              throw new NodeOperationError(this.getNode(), 'At least one recipient is required', { itemIndex: i })
            }
            if (autoFillFromOriginal && replyAll) {
              const addrs = [...(original.to ?? []), ...(original.cc ?? [])]
              for (const addr of addrs) {
                const exists = recipients.some((r) => r.email.toLowerCase() === addr.email.toLowerCase())
                if (!exists && addr.email.toLowerCase() !== identity.email.toLowerCase()) {
                  recipients.push(addr)
                }
              }
            }

            const subject = autoFillFromOriginal
              ? prefixSubject(original.subject, 'Re')
              : (this.getNodeParameter('subject', i, '') as string)
            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to: recipients,
              subject,
              keywords: { $draft: true },
              mailboxIds: { [draftMailboxId]: true }
            }
            const cc = parseCsvEmails(composeOptions.cc ?? '')
            const bcc = parseCsvEmails(composeOptions.bcc ?? '')
            if (cc.length > 0) createEmail.cc = cc
            if (bcc.length > 0) createEmail.bcc = bcc

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
            if (uploaded.emailAttachments.length > 0) createEmail.attachments = uploaded.emailAttachments

            const replyResponse = await callJmap(
              this,
              token,
              session,
              createAsDraft ? [JMAP_CORE, JMAP_MAIL] : [JMAP_CORE, JMAP_MAIL, JMAP_SUBMISSION],
              createAsDraft
                ? [['Email/set', { accountId: mailAccountId, create: { replyDraft: createEmail } }, 'c1']]
                : [
                    ['Email/set', { accountId: mailAccountId, create: { replyDraft: createEmail } }, 'c1'],
                    [
                      'EmailSubmission/set',
                      {
                        accountId: submissionAccountId,
                        create: { submit: { identityId: identity.id, emailId: '#replyDraft' } }
                      },
                      's1'
                    ]
                  ]
            )

            const emailSetResult = methodResultByCallId<JmapSetResult>(replyResponse, 'c1', 'Email/set')
            const createdId = firstCreatedId(emailSetResult)
            if (createdId == null) {
              const notCreated = JSON.stringify(emailSetResult.notCreated ?? {})
              throw new NodeOperationError(this.getNode(), `Reply could not be created. notCreated: ${notCreated}`, { itemIndex: i })
            }
            if (createAsDraft) {
              returnData.push({
                json: withDebugData({
                  success: true,
                  draftId: createdId,
                  submitted: false,
                  uploadedAttachments: uploaded.uploadedAttachments
                }, includeJmapResponse, replyResponse),
                pairedItem: { item: i }
              })
            } else {
              const submissionResult = methodResultByCallId<JmapSetResult>(replyResponse, 's1', 'EmailSubmission/set')
              if (firstCreatedId(submissionResult) == null) {
                const notCreated = JSON.stringify(submissionResult.notCreated ?? {})
                throw new NodeOperationError(this.getNode(), `Reply submission failed. notCreated: ${notCreated}`, { itemIndex: i })
              }

              let draftCleanup: { success: boolean, error?: string } = { success: true }
              try {
                await destroyEmailById(this, token, session, mailAccountId, createdId)
              } catch (error) {
                draftCleanup = { success: false, error: (error as Error).message }
              }

              returnData.push({
                json: withDebugData({
                  success: true,
                  replyMessageId: createdId,
                  submitted: true,
                  draftCleanup,
                  uploadedAttachments: uploaded.uploadedAttachments
                }, includeJmapResponse, replyResponse),
                pairedItem: { item: i }
              })
            }
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
