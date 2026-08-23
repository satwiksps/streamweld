// @ts-check
import { defineConfig } from 'astro/config';
import mermaid from 'astro-mermaid';
import starlight from '@astrojs/starlight';

const readTheDocsCanonical = process.env.READTHEDOCS_CANONICAL_URL;
const readTheDocsURL = readTheDocsCanonical === undefined ? undefined : new URL(readTheDocsCanonical);
const isReadTheDocs = process.env.READTHEDOCS === 'True';
const readTheDocsBase = isReadTheDocs
	? `/${process.env.READTHEDOCS_LANGUAGE ?? 'en'}/${process.env.READTHEDOCS_VERSION ?? 'latest'}`
	: readTheDocsURL?.pathname;
const site = process.env.STREAMWELD_DOCS_SITE ?? readTheDocsURL?.origin ?? 'http://localhost:4321';
const requestedBase = process.env.STREAMWELD_DOCS_BASE ?? readTheDocsBase ?? '/';
const trimmedBase = requestedBase.replace(/^\/+|\/+$/g, '');
const base = trimmedBase === '' ? '/' : `/${trimmedBase}`;
const socialImage = new URL(`${base === '/' ? '' : base}/og.png`, site).toString();

// https://astro.build/config
export default defineConfig({
	site,
	base,
	trailingSlash: 'always',
	outDir: './astro-dist',
	integrations: [
		mermaid({
			autoTheme: true,
			enableLog: false,
			mermaidConfig: { flowchart: { curve: 'linear' } },
		}),
		starlight({
			title: 'Streamweld',
			description: 'Durable token streams for self-hosted LLM inference.',
			favicon: '/og.png',
			components: { Head: './src/components/Head.astro' },
			customCss: ['./src/styles/custom.css'],
			lastUpdated: true,
			head: [
				{ tag: 'meta', attrs: { property: 'og:type', content: 'website' } },
				{ tag: 'meta', attrs: { property: 'og:image', content: socialImage } },
				{ tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
				{ tag: 'meta', attrs: { name: 'theme-color', content: '#0b1319' } },
			],
			social: [{ icon: 'github', label: 'GitHub', href: 'https://github.com/satwiksps/streamweld' }],
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
					label: 'Repository source',
					collapsed: true,
					items: [
						{ label: 'Normative protocol', slug: 'source/protocol' },
						{ label: 'Operations guide', slug: 'source/operations' },
						{ label: 'Client guide', slug: 'source/client' },
						{ label: 'Generated chaos evidence', slug: 'source/benchmarks' },
						{ label: 'Good first issues', slug: 'source/good-first-issues' },
					],
				},
				{
					label: 'Architecture decisions',
					collapsed: true,
					items: [{ autogenerate: { directory: 'source/decisions' } }],
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
