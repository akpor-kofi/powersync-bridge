import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(dirname(fileURLToPath(import.meta.url)));
const destination = mkdtempSync(join(tmpdir(), 'powersync-bridge-release-'));
const directories = ['core', 'react', 'powersync', 'adapters/drizzle'];
const manifests = directories.map((directory) => JSON.parse(readFileSync(join(root, 'packages', directory, 'package.json'), 'utf8')));
if (new Set(manifests.map(({ version }) => version)).size !== 1) throw new Error('All bridge packages must use the same release version.');
const dryRun = process.argv.includes('--dry-run');
const pnpmCli = process.env.npm_execpath;
const run = (command, args, cwd = root) => {
  const result = spawnSync(command, args, { cwd, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed with exit code ${result.status}`);
};

// Tar/gzip headers can differ between packers even when every packaged file is identical.
// Compare complete file contents before skipping an existing version; never overwrite it.
const sameContents = (left, right) => {
  const entries = (path) => {
    const result = spawnSync('tar', ['-tzf', path], { encoding: 'utf8' });
    if (result.status !== 0) throw new Error('Unable to inspect package archive.');
    return result.stdout.trim().split('\n').filter((name) => !name.endsWith('/')).sort();
  };
  const names = entries(left);
  if (JSON.stringify(names) !== JSON.stringify(entries(right))) return false;
  return names.every((name) => {
    if (!name.startsWith('package/')) throw new Error('Unexpected package archive path.');
    const a = spawnSync('tar', ['-xOf', left, '--', name], { maxBuffer: 32 * 1024 * 1024 });
    const b = spawnSync('tar', ['-xOf', right, '--', name], { maxBuffer: 32 * 1024 * 1024 });
    if (a.status !== 0 || b.status !== 0) throw new Error('Unable to compare package archive.');
    return a.stdout.equals(b.stdout);
  });
};

// Inherit the caller's terminal: npm can request 2FA without an OTP in arguments, logs or files.
for (const directory of directories) {
  const args = ['pack', '--pack-destination', destination];
  // Use the same pnpm that launched this release; different packer versions can rewrite peers differently.
  if (pnpmCli?.includes('pnpm')) run(process.execPath, [pnpmCli, ...args], join(root, 'packages', directory));
  else run('pnpm', args, join(root, 'packages', directory));
}
for (const artifact of readdirSync(destination).filter((name) => name.endsWith('.tgz')).sort()) {
  const path = join(destination, artifact);
  const manifest = manifests.find(({ name, version }) => `${name.replace(/^@/, '').replaceAll('/', '-')}-${version}.tgz` === artifact);
  if (!manifest) throw new Error(`Unexpected release artifact: ${artifact}`);
  const existing = spawnSync('npm', ['view', `${manifest.name}@${manifest.version}`, 'dist.integrity', '--json'], { cwd: root, encoding: 'utf8' });
  if (existing.status === 0) {
    const integrity = `sha512-${createHash('sha512').update(readFileSync(path)).digest('base64')}`;
    if (JSON.parse(existing.stdout) !== integrity) {
      const metadata = spawnSync('npm', ['view', `${manifest.name}@${manifest.version}`, 'dist.tarball', '--json'], { cwd: root, encoding: 'utf8' });
      if (metadata.status !== 0) throw new Error('Unable to read published artifact location.');
      const response = await fetch(JSON.parse(metadata.stdout));
      if (!response.ok) throw new Error(`Unable to download published artifact: ${response.status}`);
      const publishedPath = join(destination, `published-${artifact}`);
      writeFileSync(publishedPath, Buffer.from(await response.arrayBuffer()));
      if (!sameContents(path, publishedPath)) throw new Error(`${manifest.name}@${manifest.version} already exists with different contents; choose a new version.`);
    }
    console.log(`${manifest.name}@${manifest.version} is already published with identical package contents.`);
    continue;
  }
  if (!existing.stderr.includes('E404')) throw new Error(existing.stderr || 'Unable to check the npm registry.');
  run('npm', ['publish', path, '--access', 'public', '--tag', 'alpha', '--ignore-scripts', ...(dryRun ? ['--dry-run'] : [])]);
}
