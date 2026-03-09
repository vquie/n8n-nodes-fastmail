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
  bodyValues?: Record<string, unknown>
}

interface ThreadRecord {
  id: string
  emailIds?: string[]
}

interface EmailSetResult {
  created?: Record<string, { id: string }>
  updated?: Record<string, unknown>
  notUpdated?: Record<string, unknown>
  destroyed?: string[]
  notDestroyed?: Record<string, unknown>
}

const JMAP_CORE = 'urn:ietf:params:jmap:core'
const JMAP_MAIL = 'urn:ietf:params:jmap:mail'
const JMAP_SUBMISSION = 'urn:ietf:params:jmap:submission'
const FASTMAIL_ICON_DATA_URL = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEAAAABACAYAAACqaXHeAAAABGdBTUEAALGPC/xhBQAAACBjSFJNAAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAAeGVYSWZNTQAqAAAACAAEARoABQAAAAEAAAA+ARsABQAAAAEAAABGASgAAwAAAAEAAgAAh2kABAAAAAEAAABOAAAAAAAAANgAAAABAAAA2AAAAAEAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAQKADAAQAAAABAAAAQAAAAADL3X5jAAAACXBIWXMAACE4AAAhOAFFljFgAAACnGlUWHRYTUw6Y29tLmFkb2JlLnhtcAAAAAAAPHg6eG1wbWV0YSB4bWxuczp4PSJhZG9iZTpuczptZXRhLyIgeDp4bXB0az0iWE1QIENvcmUgNi4wLjAiPgogICA8cmRmOlJERiB4bWxuczpyZGY9Imh0dHA6Ly93d3cudzMub3JnLzE5OTkvMDIvMjItcmRmLXN5bnRheC1ucyMiPgogICAgICA8cmRmOkRlc2NyaXB0aW9uIHJkZjphYm91dD0iIgogICAgICAgICAgICB4bWxuczp0aWZmPSJodHRwOi8vbnMuYWRvYmUuY29tL3RpZmYvMS4wLyIKICAgICAgICAgICAgeG1sbnM6ZXhpZj0iaHR0cDovL25zLmFkb2JlLmNvbS9leGlmLzEuMC8iPgogICAgICAgICA8dGlmZjpYUmVzb2x1dGlvbj4yMTY8L3RpZmY6WFJlc29sdXRpb24+CiAgICAgICAgIDx0aWZmOllSZXNvbHV0aW9uPjIxNjwvdGlmZjpZUmVzb2x1dGlvbj4KICAgICAgICAgPHRpZmY6UmVzb2x1dGlvblVuaXQ+MjwvdGlmZjpSZXNvbHV0aW9uVW5pdD4KICAgICAgICAgPGV4aWY6UGl4ZWxZRGltZW5zaW9uPjI1NjwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAgICAgIDxleGlmOlBpeGVsWERpbWVuc2lvbj4yNTY8L2V4aWY6UGl4ZWxYRGltZW5zaW9uPgogICAgICAgICA8ZXhpZjpDb2xvclNwYWNlPjE8L2V4aWY6Q29sb3JTcGFjZT4KICAgICAgPC9yZGY6RGVzY3JpcHRpb24+CiAgIDwvcmRmOlJERj4KPC94OnhtcG1ldGE+Cle0H4IAAA+ESURBVHgB7Vt7cBT1Hf/+frt3lwsPEUhIAPEVZBxfIGAV0BYBowEUWk0Vq/JHRQiCOmB9YDVaobaFcQQTKs60ziBqQbFVCSoIM/KolShUx398wAgMJCGhPERyd7v76+e7e0v2du+SyxNm2t/MZXd/r+/78XuE6H+8iC6hv1xJqt9QQKYYREIMJEsVkJC9SFk5NnwhG/F+hKSoIaX2k6b2Ut8JNVQurM7Gr/MYMOPjQtLjo4nkWBA3EoRdgGcf0sIE4h26XOgqSaYCvWac2xvAqN0kaAdaNpNhbKMVNx7sDGa4KHTM3NM351COOR7I3wmCryep5dvEKpPIwk8xpS61mUACJYGf1PDEj5limXWo24SKV+mk+IheGduYaXRr6zuGATOqc0k/fgdZVhmkdqWNvJlwkG8tRun6s8ZooSQT6XOSspKMH16nFZN/TNe9NXXtZ0DZpp8D4AJI+kpbWkx4ZxZmBDNEmZ+DIwupcsLa9oBrOwNmrj+PRPj3AH67rbJZEw6QrOL88xY2j6xMJDmIGcH9hXidhHqMKsZ9750u23cfFlkOm7VxCoz0Baj6IDJiGNSMXdv2rANR154TBrofA+I/YCAP5hIBMd1hPj1JhvQmv2EkmeJ0Cv4F+noEimDshfnNpZcm/CPYp/maVjJACSrb9DimLAcROgBnnl2CaP4Z8RMIbzvRcRsw/YxE6BsytFrQDvs9O2kv/4E4I7kkYwiPqghzjwATRiFcDiM93M2G0xIsIRIwwXJaPp61shmJpKKcPQNmVIdIO8pSn2WHKltdUyezv1g1Gb7Fzkq9Bqm+SxXjv07Ts6UqQbO3DCbVeDNZ8g6ScK6YjDKZGmsah1jTqCSr14O0YkRWzig7BjDx8vDLULd7KJFB5d2wRdY/oY5LKJR4j5aVuCreErHNt8+pilAiZxIYOg9qdQ0cIBiMX6CAnBBMwki8AibMyIYJLTNAsdpvrAR3Z1IiQ/h17PAg7Pxp0k6+0mGE+wlkRpjR6fAXTyEUFjr+x98J3zoSTDO+HOYwG76mWXNomQEzNyxADH4WtoyZ/XNhuA61U2YVaTSXlo37Lg06HV9V9j78RGgpzPGmZvGy4gto+Q2LmkOgeQbM2nQzzO4tOCHdCVGeqTgWs9or609Uf/gJWlPKHOq6Ur46TLV9FkLC820n6fdJDn6cjf2CKse9mwmxzAx4YMMgiontADAg4O3tOK5ZYMpv6M/jl2SavEvqZ26chyjzR/gEGRASRyHT3E/HfhxFb0zZlw6f5KrE3wS7j6nFUP0g8eyJBSYm9fBpJ57RZgFYEAQTy7i5ha01Eaf+Bb0HTp14+WIwx9PodkoZ0VRJTqKzFpLHIJ/d2w4vsZiWT3jYM+L0v878EAILz7Mdo6UopEsaVpRPwwb3o3BORFkJa2rFxL6BRCmoAbywUeppMDMT8e9Tff8Fp59iHwYFAx6HwN5HokUFvbvRLaOK6OqLC0njZYOlhKXMp2e8eyDXNwrphb/oDXdA9S8PJBycyppGLWlY8a25pGsdnh/HdN/lwCnWOPuqIXm1U669CEzIpYTJbgpowxT0cPiKkKbf7h+aygBez1tiDhIZfz/H4wt6kl4s3hNsPENq/jJp93WXDXgqFA6TCTPwFstmhpozffMeZxcq2ZjKgKgah9B2BVm+LJLTWzPxL4S7V7yTnonv3xee/VcjHvtUCyE/8RQLeYyU2tDoiZxxnmqfCSjjbnvV5u3B78xMKZ/r8ljvxyOL7zWXiLjUxHP+tICHCg0LTSHu8k7TpAFlmwvQMC5g+xxeLGMX9Sus8g70vqtPcsaq7ZEib11nvl/zs+Ki4T8tHpsJhlTHqpRp7JI6L8yaCvsCMGZc2bo6ptUuTQxQiWuxFu9j7+q4rfy04yutJHYymUpclVCIPlbbw9Mydemo+uHXlUwzhP4xKVGSac5lJYNjiPorNUjcWxQWUDCNvohvY9z6JgaQuN7eiHBb+MkZnxn/AZ7/HW914F2IRhhTIQasUttzVqmt0UGBPu2sGD5+8qCRP5v4KjLcVVDjQiCeYWXmABKaeAe+4AT6pkAWdvoOX5csDgPKN3Ouj61r3wYHS1/RLqoobnmRw0P5p6lpJKwtanu0w7SBpS4S5hZI7k6FCKXSGbhLUfJZUZzHOO/0m4EygKSikeVMM4rDgPpEPirPD4Q/jv2CtkITUmNKEkjgwb0cuUADLGhDuF3aMHxMUuqCVgHrQZZpBkBmqoDkFfDeKn1mYGFnSQl1fv2oS/N5rMMAMwSExdkB++c9eaJq/tOqwprAkVQTjjZsbb022FLXIHWC1IFHNlL344jVUbWfaaxBTKuKWedy/6SXSJwD7yDsra5Ts8B2LN7A1L89VdXaF9YGnQaRhDZsC5eQ0h4XY07ubW4alrrUjUXQ8jtZV610SVlzE3jaoMDfmkbcgCakLOc1HbRaxkDu6miAc1bnGYpXdh68e6vidakNrfxytUHHaVELvqFJ6rLNUvdiFyNZp0gdS+cIYU12KHQ0QGi9vAPtd8d7HqewdSLQ1pYK1gYN2iCC2sAeXhrWIiDLR2rtkroXtYglT5gkjwshe2PWU00cF4QUNs2OBgjK4bVfoCjs28dMlmHHFPZh7BtYG6S5XX2s/eqK0ZN+aXt4tvUsPXy2yMTMGKt/2o1ZLHbtNUHSB2Q7ZQf1g/7VH4sOeKGqaKWULJmOk3prMXQYoBC80gU6gRObsC+OtBaCt78EEESnqs8Laen6wbT/cJRyQiZrfaeUiBbRTcuI2P7MB0FIZQfsJAPUEV87hAKslOpBMdkNbccC7a2tCJtU0xAF4UW0bhcOgDCeie/MEjt5vJsWifTwC5f5rRLmUYbtMEBK3MxochI2UswAPqujMCcMB+26tvzRbHBU9Zkj9X1gQhTM6IoSze2ZlzDjPf05BPsaoUubJocBFq6lkMGYelwhPmUY53+xItT/u00Iu1LHNv66nYWYXHUZ8YyvZSaKZCism/FUP4jcQElJoNnVACH2QgOOIDakZoO8ty704ej3FnfOuri2Xg2pg/h9DbmUA2Z4uJv1VO3paAkxQsPix6tvApRDuY+YhmYnZE4YLJA4rVV7sOmRCs8+g7OuxYjscWepH43QY29cit9lVHMkx5Z69hOkotDWL6i9AKFj/KmwxAIPuOwp6P4laHYzwfKxHC93OPv9HpB8JC3EUJr9wYWe2vSvbOs4K2Fbn758JL3zWX/S8a3bPiD9kM6snbu+7gIIdZhlpG7v8a4QOLCjfCxoRsFXsgjtI5jBfe6n/WRHqEe6Y40wGd/Pp7R5P5QVOXw8l5a8XURVO+HhweJuEXYpWE12QsHeHuzbq9hBIBbJydgJ7p7gawjeAq1GErTJrWpigBnaSrIR19Nk6q4Qa4FSd1H5VxWZdoU+3X32zooNQ9Z+8nUP6h417AUGNmE7pbCRYuqTIHB1JgDlX6lw7e66u/nOhLew/WNbrCGsa7zKtEuqac7c8DesCksDx84yhFTNnNqWKyguoK58zl5XfwuW4m9biXgKfXokSlY8tqZiUr9SFx+f1xMrQajb1vTkUyJBjxJflDjDy4xqBWkZjwLhFOIZbd4ThHNc6SUhlQGN2kZsi32BzVFvHz4TgLvUryajfnpqw5n3Faqtv0fTI1eb9k2WJvwk7jGYZuKLQycTG5tq3Sjg1vANTCGWBcMhOsCb9u9/1jMz3z54ntv9THvOXH/wPATsZ8w0C1ipafBN2rI1peec9OKdqgHc0k29jlsXX8IXOP1wxKRrgq4akk9TRg8p0CJaBauZd5Iz4Z1xkkqrwG5PIc4EUlDiUyJkg19GdfP1lAZ8BBmwuPgEjlCe4nSJTGWftE4ZdSH9BCetZGKPRQ+X6LV1z/onOt3fobpDv9OAmxFLETDQgivgiExUvri4ILC5E3AUdldkUdG5m1ZfPnjArUPP7WmftbuHjby9BCbAn8TnVU4syJwb2BN1zZ+ydTUPSS28BOd/yP4cal3Iek4u5BZ7s3JiXinMO7URnYIawCPRcdqNF8+/6qI+B/wnrQwA2RV2mUKLy9YdesgFdLqes6sOPci4ME5+4iXMGKHwgBCh+emIt0ltDvFZVbVTNKmvsQxD561pb+GkQtgHJ+oPifw+v10xwpd1eDt3wjvbPKs9zPURxXv9vt1jSAiaqhvYELlteUm/v2dCIb0GJHvzQGWaT2vhCGtFyhwMEMtNBvJIqL5h7ayNRy5I6dCJH7PeO3JBqK5hLUzxEcbBTzzjyjhjS/2Z5ohnFJtlAHfI25G3CEBeCiGLChSYAzsd5OaTZMLYMuf9hntvW/1V6sF8YFDbK3juOVUN90rN2ALGT7Idns/meXbG1TJiK/I/7buwJWipYs3QmwHndc97WQ9F7k40spcN+BLcq8AyE7EWarENarG479FeVeWlIvOJcgZY6arLAb/+rP4loGo+AI2GVgJMaqhzxgkK5URxOSy+sv5E3a/XlDZzop0ElBUDuC8zIb9H/lI4lvt4h8XvE5LzIX3gm+/wF4qqsSJ/TTfUe0sn53/jtrfmOffdusGGLibBd+OITYxgu/ZneO583GarvZFYUfdD3ZxsiOexWTOAO8PLitnr65+QUj6Jd92/1uY+bkFMtjUCqy8cTNBOQNqmSFZrwvzONEWtJrQf9V697eWaceRwyFRmrqapfkg9LkQePwInmKMBYxiSmB4scWxjuVMHnnwCjPBssM1X3NT3WftgNNArfUWrGOBOASZMxcClQgsNdBKPoEm4fR1vjMvmMA9ew8OfJHCx8Rhudx4H1s5mnVLwWNiBlqInNCx0qi/S70ya5swvsF0RxVVlYz9M4oHKSf1a/e8zbWIAA7//g5rzlQo9h41OLC1xrorrJ1kVqAMnU8wYb2FC7Tiexql5+7nvrGF8xgCtWo25Hn2xuNcet601zzYzwAVSVtVwK2hZwDewWlJVd0x7nq5pWZa5CwufhZXFfd5sz3ztZgADn/9BTbeYCk2DDZZBGkM5GrCzCsTnNmLKSZftXGFC0JRdUKDlOdJclS63by2IDmGAC/Sh1fuiiR45E5TA8bZpXI9EpS+rupOsAHn78mJmf+HMA/OQ/MPyFaksmwZy/HpsZm4SyloVOt644XnfktaF35ZnhzLAi8D9Hx7qryw5Bj5irGWqkbiRAJ+hekuESTuN9nbGO7PFzi6hOfARh3G/ZY/U5A48NwtpbX3xhrwDviEd8tlpDPBid9tqpQ3s01AQO2meC+IGog1bx7iTIJRzbVXhlpky+XyyBkzaH4n2+H5/Q7RmTalIsz/nnfn/7+3mwH8BCN0vQiN1+CIAAAAASUVORK5CYII='

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
    simplified.bodyValues = email.bodyValues ?? {}
  }

  return simplified
}

function firstCreatedId (result: EmailSetResult): string | null {
  const first = Object.values(result.created ?? {})[0]
  return first?.id ?? null
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
          'preview',
          'messageId',
          'inReplyTo'
        ],
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
          'preview',
          'messageId',
          'inReplyTo'
        ],
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

async function sleep (ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms))
}

export class Fastmail implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Fastmail',
    name: 'fastmail',
    group: ['transform'],
    version: 1,
    icon: 'file:Fastmail.png',
    iconUrl: FASTMAIL_ICON_DATA_URL,
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
          { name: 'Send a Message', value: 'send', action: 'Send a message' },
          { name: 'Send Message and Wait for Response', value: 'sendAndWait', action: 'Send a message and wait for a response' }
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
            operation: ['send', 'sendAndWait', 'reply', 'create']
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
            operation: ['send', 'sendAndWait', 'create']
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
            operation: ['send', 'sendAndWait', 'create']
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
            operation: ['send', 'sendAndWait', 'create']
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
            operation: ['send', 'sendAndWait', 'create']
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
            operation: ['send', 'sendAndWait', 'create', 'reply']
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
            operation: ['send', 'sendAndWait', 'create', 'reply']
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
      {
        displayName: 'Wait Timeout (Seconds)',
        name: 'waitTimeoutSeconds',
        type: 'number',
        default: 120,
        typeOptions: {
          minValue: 5
        },
        displayOptions: {
          show: {
            resource: ['message'],
            operation: ['sendAndWait']
          }
        }
      },
      {
        displayName: 'Poll Interval (Seconds)',
        name: 'pollIntervalSeconds',
        type: 'number',
        default: 10,
        typeOptions: {
          minValue: 2
        },
        displayOptions: {
          show: {
            resource: ['message'],
            operation: ['sendAndWait']
          }
        }
      }
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

            if (emails.length === 0) {
              returnData.push({ json: { count: 0, message: 'No messages found' }, pairedItem: { item: i } })
            } else {
              for (const email of emails) {
                returnData.push({ json: simplifyEmail(email, includeBodyValues), pairedItem: { item: i } })
              }
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

            const result = methodResult<EmailSetResult>(response, 'Email/set')
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

          if (operation === 'send' || operation === 'sendAndWait') {
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

            const createEmail: JsonObject = {
              from: [{ email: identity.email, name: identity.name }],
              to,
              subject
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
              ['Email/set', { accountId: mailAccountId, create: { outgoing: createEmail } }, 'c1'],
              ['EmailSubmission/set', { accountId: submissionAccountId, create: { submit: { identityId, emailId: '#outgoing' } } }, 's1']
            ])

            if (operation === 'send') {
              returnData.push({
                json: {
                  success: true,
                  sentMessageId: firstCreatedId(methodResult<EmailSetResult>(sendResponse, 'Email/set'))
                },
                pairedItem: { item: i }
              })
              continue
            }

            const sentMessageId = firstCreatedId(methodResult<EmailSetResult>(sendResponse, 'Email/set'))
            if (sentMessageId == null) {
              throw new NodeOperationError(this.getNode(), 'Message was sent but could not read the sent message ID', { itemIndex: i })
            }

            const sentEmail = await getEmailById(this, token, session, mailAccountId, sentMessageId)
            if (sentEmail?.threadId == null) {
              throw new NodeOperationError(this.getNode(), 'Could not resolve thread for sent message', { itemIndex: i })
            }

            const timeoutSeconds = this.getNodeParameter('waitTimeoutSeconds', i, 120) as number
            const pollSeconds = this.getNodeParameter('pollIntervalSeconds', i, 10) as number
            const started = Date.now()
            let replyFound: EmailRecord | null = null

            while ((Date.now() - started) < timeoutSeconds * 1000) {
              await sleep(pollSeconds * 1000)

              const threadEmailIds = await getThreadEmailIds(this, token, session, mailAccountId, sentEmail.threadId)
              const threadEmails = await getEmailsByIds(this, token, session, mailAccountId, threadEmailIds)
              replyFound = threadEmails.find((email) => {
                if (email.id === sentMessageId) return false
                const fromEmail = email.from?.[0]?.email?.toLowerCase()
                if (fromEmail == null) return false
                return fromEmail !== identity.email.toLowerCase()
              }) ?? null

              if (replyFound != null) break
            }

            returnData.push({
              json: {
                sentMessageId,
                threadId: sentEmail.threadId,
                timedOut: replyFound == null,
                reply: (replyFound != null) ? simplifyEmail(replyFound, false) : null
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
              subject
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
              ['Email/set', { accountId: mailAccountId, create: { reply: createEmail } }, 'c1'],
              ['EmailSubmission/set', { accountId: submissionAccountId, create: { submit: { identityId, emailId: '#reply' } } }, 's1']
            ])

            returnData.push({
              json: {
                success: true,
                replyMessageId: firstCreatedId(methodResult<EmailSetResult>(replyResponse, 'Email/set'))
              },
              pairedItem: { item: i }
            })
            continue
          }
        }

        if (resource === 'label') {
          if (operation === 'getMany') {
            const mailboxes = await getMailboxes(this, token, session, mailAccountId)
            if (mailboxes.length === 0) {
              returnData.push({ json: { count: 0, message: 'No labels found' }, pairedItem: { item: i } })
            } else {
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
            const created = methodResult<EmailSetResult>(response, 'Mailbox/set')
            returnData.push({
              json: {
                success: true,
                labelId: firstCreatedId(created),
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
            const result = methodResult<EmailSetResult>(response, 'Mailbox/set')
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
                draftId: firstCreatedId(methodResult<EmailSetResult>(response, 'Email/set'))
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

            if (drafts.length === 0) {
              returnData.push({ json: { count: 0, message: 'No drafts found' }, pairedItem: { item: i } })
            } else {
              for (const draft of drafts) {
                returnData.push({ json: simplifyEmail(draft, includeBodyValues), pairedItem: { item: i } })
              }
            }
            continue
          }

          if (operation === 'delete') {
            const draftId = this.getNodeParameter('draftId', i) as string
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Email/set', { accountId: mailAccountId, destroy: [draftId] }, 'd1']
            ])
            const result = methodResult<EmailSetResult>(response, 'Email/set')
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
            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Thread/get', { accountId: mailAccountId, ids: [threadId] }, 't1']
            ])
            const thread = methodResult<{ list?: ThreadRecord[] }>(response, 'Thread/get').list?.[0]
            if (thread == null) {
              returnData.push({ json: { message: 'Thread not found', threadId }, pairedItem: { item: i } })
            } else {
              returnData.push({
                json: {
                  id: thread.id,
                  emailIds: thread.emailIds ?? [],
                  messageCount: (thread.emailIds ?? []).length
                },
                pairedItem: { item: i }
              })
            }
            continue
          }

          if (operation === 'getMany') {
            const limit = this.getNodeParameter('limit', i, 25)
            const filterLabelId = this.getNodeParameter('filterLabelId', i, '') as string
            const filter = filterLabelId ? { inMailbox: filterLabelId } : {}

            const queryResponse = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Thread/query', { accountId: mailAccountId, filter, limit }, 'tq1']
            ])
            const ids = methodResult<{ ids?: string[] }>(queryResponse, 'Thread/query').ids ?? []

            if (ids.length === 0) {
              returnData.push({ json: { count: 0, message: 'No threads found' }, pairedItem: { item: i } })
              continue
            }

            const response = await callJmap(this, token, session, [JMAP_CORE, JMAP_MAIL], [
              ['Thread/get', { accountId: mailAccountId, ids }, 't1']
            ])
            const threads = methodResult<{ list?: ThreadRecord[] }>(response, 'Thread/get').list ?? []

            for (const thread of threads) {
              returnData.push({
                json: {
                  id: thread.id,
                  emailIds: thread.emailIds ?? [],
                  messageCount: (thread.emailIds ?? []).length
                },
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
              const result = methodResult<EmailSetResult>(response, 'Email/set')
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
            const result = methodResult<EmailSetResult>(response, 'Email/set')
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
              subject
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
              ['Email/set', { accountId: mailAccountId, create: { reply: createEmail } }, 'c1'],
              ['EmailSubmission/set', { accountId: submissionAccountId, create: { submit: { identityId, emailId: '#reply' } } }, 's1']
            ])

            returnData.push({
              json: {
                success: true,
                replyMessageId: firstCreatedId(methodResult<EmailSetResult>(replyResponse, 'Email/set'))
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
