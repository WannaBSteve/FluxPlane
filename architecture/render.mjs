import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
const here = dirname(fileURLToPath(import.meta.url));
const skill = process.env.ARCH_DIAGRAM_SKILL || join(homedir(), '.codex/skills/arch-diagram');
execFileSync(process.execPath, [join(here, 'build-spec.mjs')], { stdio: 'inherit' });
const output = join(here, 'fluxplane.html');
execFileSync(process.execPath, [join(skill, 'scripts/build.mjs'), join(here, 'fluxplane.spec.json'), output], { stdio: 'inherit' });
const license = readFileSync(join(here, 'licenses/arch-diagram-MIT.txt'), 'utf8');
const theme = readFileSync(join(here, 'theme.css'), 'utf8');
writeFileSync(output, readFileSync(output, 'utf8')
  .replace('<head>', '<head>\n<!-- Renderer: https://github.com/bassnova42/arch-diagram-skill\n' + license + '\n-->')
  .replace('</head>', `<style>${theme}</style>\n</head>`));
