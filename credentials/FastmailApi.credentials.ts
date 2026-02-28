import type { ICredentialType, INodeProperties } from 'n8n-workflow';

export class FastmailApi implements ICredentialType {
	name = 'fastmailApi';
	displayName = 'Fastmail API';
	documentationUrl = 'https://www.fastmail.help/hc/en-us/articles/360060591073-API-Access';
	properties: INodeProperties[] = [
		{
			displayName: 'API Token',
			name: 'token',
			type: 'string',
			default: '',
			required: true,
			typeOptions: {
				password: true,
			},
		},
	];

	authenticate = {
		type: 'generic' as const,
		properties: {
			headers: {
				Authorization: '=Bearer {{$credentials.token}}',
			},
		},
	};

	test = {
		request: {
			baseURL: 'https://api.fastmail.com',
			url: '/.well-known/jmap',
		},
	};
}
