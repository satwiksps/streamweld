import { access, mkdir, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const docsSiteRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const monorepoRoot = path.resolve(docsSiteRoot, '..', '..');
const sourceSnapshotRoot = path.join(docsSiteRoot, '.repository');
const repositoryRoot = await containsSourceDocuments(monorepoRoot) ? monorepoRoot : sourceSnapshotRoot;
const outputRoot = path.join(docsSiteRoot, 'src', 'content', 'docs', 'source');
const repositoryURL = 'https://github.com/satwiksps/streamweld';

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
	const linkedBody = rewriteRelativeRepositoryLinks(body, source);
	const editUrl = `${repositoryURL}/edit/main/${source}`;
	const generated = [
		'---',
		`title: ${JSON.stringify(title)}`,
		`editUrl: ${JSON.stringify(editUrl)}`,
		'---',
		'',
		`# ${title}`,
		'',
		'> This page is generated at build time from the repository source linked above.',
		'',
		linkedBody.trimStart(),
	].join('\n');

	const destinationPath = path.join(outputRoot, ...destination.split('/'));
	await mkdir(path.dirname(destinationPath), { recursive: true });
	await writeFile(destinationPath, generated, 'utf8');
}

function rewriteRelativeRepositoryLinks(markdown, source) {
	const sourceURL = new URL(`${repositoryURL}/blob/main/${source}`);
	return markdown.replace(/(\]\()(\.{1,2}\/[^)\s]+)(?=[\s)])/g, (_match, opening, target) => {
		return `${opening}${new URL(target, sourceURL)}`;
	});
}

async function containsSourceDocuments(candidate) {
	try {
		await access(path.join(candidate, 'docs', 'protocol.md'));
		await access(path.join(candidate, 'benchmarks', 'results.md'));
		return true;
	} catch {
		return false;
	}
}
