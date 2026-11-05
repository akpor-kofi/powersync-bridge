import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtempSync, readFileSync, readdirSync } from 'node:fs';
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
    if (JSON.parse(existing.stdout) !== integrity) throw new Error(`${manifest.name}@${manifest.version} already exists with different contents; choose a new version.`);
    console.log(`${manifest.name}@${manifest.version} is already published with this integrity.`);
    continue;
  }
  if (!existing.stderr.includes('E404')) throw new Error(existing.stderr || 'Unable to check the npm registry.');
  run('npm', ['publish', path, '--access', 'public', '--tag', 'alpha', '--ignore-scripts', ...(dryRun ? ['--dry-run'] : [])]);
}
