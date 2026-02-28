import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';

interface Identity {
	id: string;
	email: string;
	name?: string;
	replyTo?: Array<{
		email: string;
		name?: string;
	}>;
}

interface SessionResponse {
	apiUrl: string;
	username?: string;
	primaryAccounts: Record<string, string>;
}

interface JmapMethodResponse<T> {
	methodResponses?: Array<[string, T, string]>;
}

interface IdentityGetResponse {
	list?: Identity[];
}

function getMethodResponse<T>(response: JmapMethodResponse<T>, methodName: string): T {
	const methodResponse = response.methodResponses?.find(([name]) => name === methodName);

	if (!methodResponse) {
		throw new Error(`Fastmail API response did not include ${methodName}.`);
	}

	return methodResponse[1];
}

export class Fastmail implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Fastmail',
		name: 'fastmail',
		group: ['transform'],
		version: 1,
		description: 'Fetch Fastmail account and identity information via JMAP',
		defaults: {
			name: 'Fastmail',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'fastmailApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Identity Email',
				name: 'identityEmail',
				type: 'string',
				default: '',
				placeholder: 'you@example.com',
				description:
					'Optional identity email filter. If empty, all identities from the account are returned.',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const credentials = (await this.getCredentials('fastmailApi')) as { token: string };
		const token = credentials.token;
		const returnData: INodeExecutionData[] = [];

		for (let i = 0; i < items.length; i++) {
			try {
				const identityEmail = this.getNodeParameter('identityEmail', i, '') as string;
				const session = (await this.helpers.httpRequest({
					method: 'GET',
					url: 'https://api.fastmail.com/.well-known/jmap',
					headers: {
						Authorization: `Bearer ${token}`,
					},
					json: true,
				})) as SessionResponse;

				if (!session.apiUrl) {
					throw new Error('Fastmail API did not return an apiUrl.');
				}

				const accountId = session.primaryAccounts?.['urn:ietf:params:jmap:mail'];
				if (!accountId) {
					throw new Error('Fastmail API did not return a mail accountId.');
				}

				const identityGetResponse = (await this.helpers.httpRequest({
					method: 'POST',
					url: session.apiUrl,
					headers: {
						Authorization: `Bearer ${token}`,
					},
					body: {
						using: [
							'urn:ietf:params:jmap:core',
							'urn:ietf:params:jmap:mail',
							'urn:ietf:params:jmap:submission',
						],
						methodCalls: [['Identity/get', { accountId, ids: null }, 'identityCall']],
					},
					json: true,
				})) as JmapMethodResponse<IdentityGetResponse>;

				const identities = getMethodResponse(identityGetResponse, 'Identity/get').list ?? [];
				const filteredIdentities =
					identityEmail === ''
						? identities
						: identities.filter((identity: Identity) => identity.email === identityEmail);

				returnData.push({
					json: {
						accountId,
						apiUrl: session.apiUrl,
						identityCount: filteredIdentities.length,
						identities: filteredIdentities,
						username: session.username ?? null,
					},
					pairedItem: {
						item: i,
					},
				});
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: {
							error: (error as Error).message,
						},
						pairedItem: {
							item: i,
						},
					});
					continue;
				}

				throw error;
			}
		}

		return [returnData];
	}
}
