import type { ICredentialType, INodeProperties } from 'n8n-workflow'

const FASTMAIL_DEFAULT_SCOPES = [
  'offline_access',
  'urn:ietf:params:jmap:core',
  'urn:ietf:params:jmap:mail',
  'urn:ietf:params:jmap:submission'
].join(' ')

export class FastmailOAuth2Api implements ICredentialType {
  name = 'fastmailOAuth2Api'
  extends = ['oAuth2Api']
  displayName = 'Fastmail OAuth2 API'
  icon = 'file:fastmail.svg' as const
  documentationUrl = 'https://www.fastmail.com/dev/'
  properties: INodeProperties[] = [
    {
      displayName: 'Grant Type',
      name: 'grantType',
      type: 'hidden',
      default: 'authorizationCode'
    },
    {
      displayName: 'Authorization URL',
      name: 'authUrl',
      type: 'hidden',
      default: 'https://www.fastmail.com/oauth/authorize',
      required: true
    },
    {
      displayName: 'Access Token URL',
      name: 'accessTokenUrl',
      type: 'hidden',
      default: 'https://www.fastmail.com/oauth/token',
      required: true
    },
    {
      displayName: 'Scope',
      name: 'scope',
      type: 'string',
      default: FASTMAIL_DEFAULT_SCOPES,
      description: 'Space-separated OAuth scopes'
    },
    {
      displayName: 'Authentication',
      name: 'authentication',
      type: 'hidden',
      default: 'header'
    }
  ]
}
