/// <reference types="@cloudflare/workers-types" />

interface Env {
	ASSETS: Fetcher;
}

export default {
	fetch(request: Request, env: Env): Promise<Response> {
		return env.ASSETS.fetch(request);
	},
} satisfies ExportedHandler<Env>;
