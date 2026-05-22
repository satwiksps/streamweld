import { mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const docsSiteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repositoryRoot = path.resolve(docsSiteRoot, '..', '..');
const outputRoot = path.join(docsSiteRoot, 'src', 'content', 'docs', 'source');

const documents = [
	['docs/protocol.md', 'protocol.md', 'Normative protocol'],
	['docs/operations.md', 'operations.md', 'Operations guide'],
	['docs/client.md', 'client.md', 'Client guide'],
	['docs/good-first-issues.md', 'good-first-issues.md', 'Good first issues'],
	['benchmarks/results.md', 'benchmarks.md', 'Generated chaos evidence'],
];

await rm(outputRoot, { recursive: true, force: true });
await mkdir(path.join(outputRoot, 'decisions'), { recursive: true });

for (const [source, destination, fallbackTitle] of documents) {
	await renderSourceDocument(source, destination, fallbackTitle);
}

const decisionsRoot = path.join(repositoryRoot, 'docs', 'decisions');
for (const entry of await readdir(decisionsRoot, { withFileTypes: true })) {
	if (!entry.isFile() || !entry.name.endsWith('.md')) continue;
	await renderSourceDocument(
		path.posix.join('docs/decisions', entry.name),
		path.posix.join('decisions', entry.name),
		entry.name.replace(/\.md$/, ''),
	);
}

async function renderSourceDocument(source, destination, fallbackTitle) {
	const sourcePath = path.join(repositoryRoot, ...source.split('/'));
	const original = (await readFile(sourcePath, 'utf8')).replace(/\r\n/g, '\n');
	const heading = original.match(/^#\s+(.+)\n/);
	const title = heading?.[1] ?? fallbackTitle;
	const body = heading ? original.slice(heading[0].length) : original;
	const editUrl = `https://github.com/streamweld/streamweld/edit/main/${source}`;
	const generated = [
		'---',
		`title: ${JSON.stringify(title)}`,
		`editUrl: ${JSON.stringify(editUrl)}`,
		'---',
		'',
		'> This page is generated at build time from the repository source linked above.',
		'',
		body.trimStart(),
	].join('\n');

	const destinationPath = path.join(outputRoot, ...destination.split('/'));
	await mkdir(path.dirname(destinationPath), { recursive: true });
	await writeFile(destinationPath, generated, 'utf8');
}
