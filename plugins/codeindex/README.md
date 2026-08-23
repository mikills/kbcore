# codeindex skills

Agent skills for indexing a codebase with the `codeindex` CLI and searching it
through Minnow. Each skill is a directory holding a `SKILL.md` with YAML front
matter describing when it applies, which is the format most coding agents read.
Agents generally identify a skill by its directory name, so the directories are
prefixed and the `name` field matches.

| Skill | Covers |
| --- | --- |
| [`codeindex-setup`](skills/codeindex-setup/SKILL.md) | Installing the CLI and pointing it at a Minnow service |
| [`codeindex-index`](skills/codeindex-index/SKILL.md) | Indexing a repository, what gets indexed, branches and worktrees |
| [`codeindex-search`](skills/codeindex-search/SKILL.md) | Branch-aware search through codeindex's read-only MCP tools |
| [`codeindex-troubleshoot`](skills/codeindex-troubleshoot/SKILL.md) | Killed, hung, or silent runs, stale locks, missing files |

## Using them

Copy the directories where the agent looks for skills, keeping their names.
Claude Code reads `~/.claude/skills/` for every project and `.claude/skills/`
for one project:

```bash
cp -R plugins/codeindex/skills/* ~/.claude/skills/
```

Agents without skill support can read the files directly; they are plain
Markdown and stand on their own.
