// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

const onGitHubPages = process.env.GITHUB_ACTIONS === 'true';
const site = process.env.STREAMWELD_DOCS_SITE ?? (onGitHubPages ? 'https://streamweld.github.io' : 'http://localhost:4321');
const base = process.env.STREAMWELD_DOCS_BASE ?? (onGitHubPages ? '/streamweld' : '/');
const socialImage = new URL(`${base === '/' ? '' : base}/og.png`, site).toString();

// https://astro.build/config
export default defineConfig({
	site,
	base,
	outDir: './astro-dist',
	integrations: [
		starlight({
			title: 'Streamweld',
			description: 'Durable token streams for self-hosted LLM inference.',
			favicon: '/og.png',
			customCss: ['./src/styles/custom.css'],
			lastUpdated: true,
			head: [
				{ tag: 'meta', attrs: { property: 'og:type', content: 'website' } },
				{ tag: 'meta', attrs: { property: 'og:image', content: socialImage } },
				{ tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
				{ tag: 'meta', attrs: { name: 'theme-color', content: '#0b1319' } },
			],
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/streamweld/streamweld' }],
			sidebar: [
				{
					label: 'Start here',
					items: [
						{ label: 'Install in ten minutes', slug: 'getting-started' },
						{ label: 'What Streamweld guarantees', slug: 'concepts/durability' },
					],
				},
				{
					label: 'How it works',
					items: [
						{ label: 'Architecture', slug: 'concepts/architecture' },
						{ label: 'Resume and stop', slug: 'protocol/resume-and-stop' },
						{ label: 'Producer migration', slug: 'protocol/producer-migration' },
						{ label: 'HTTP and SSE', slug: 'reference/http-and-sse' },
					],
				},
				{
					label: 'Operate',
					items: [
						{ label: 'Kubernetes', slug: 'operations/kubernetes' },
						{ label: 'Production journals', slug: 'operations/production' },
						{ label: 'Observe and debug', slug: 'operations/observability' },
					],
				},
				{
					label: 'Integrate',
					items: [
						{ label: 'TypeScript clients', slug: 'sdk/typescript' },
					],
				},
				{
					label: 'Evidence and reference',
					items: [
						{ label: 'Configuration', slug: 'reference/configuration' },
						{ label: 'Compatibility probes', slug: 'reference/compatibility' },
						{ label: 'Chaos results', slug: 'reference/benchmarks' },
						{ label: 'Prior art and non-goals', slug: 'reference/prior-art' },
					],
				},
			],
		}),
	],
});
