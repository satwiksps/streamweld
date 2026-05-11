import { cloudflare } from '@cloudflare/vite-plugin';
import { sites } from '@openai/sites-vite-plugin';
import { defineConfig } from 'vite';

export default defineConfig({
	publicDir: 'astro-dist',
	plugins: [
		sites(),
		cloudflare({
			config: {
				name: 'server',
				main: 'worker/index.ts',
				compatibility_date: '2026-05-22',
				assets: {
					binding: 'ASSETS',
					not_found_handling: '404-page',
				},
			},
		}),
	],
});
