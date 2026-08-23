---
description: Scaffold a new product skill under skills/dbt-lineage/ matching the repo's SKILL.md conventions.
argument-hint: [skill-name] [one-line purpose]
---

Scaffold a new **product** skill for the `dbt-lineage` Claude Code plugin (the skills
shipped to end users of the CLI — not contributor tooling).

Requested skill: **$ARGUMENTS**

## Before writing — gather what you can't infer

Read one existing skill of the same type as a template
(`skills/dbt-lineage/skills/*/SKILL.md`) and the plugin README
(`skills/dbt-lineage/README.md`). Then determine — asking the user only if not
already clear from the request:

1. **Slug** — kebab-case, becomes both the directory name and frontmatter `name`.
2. **Type** — one of:
   - `command` — wraps a specific CLI invocation, deterministic (e.g. `trace-column`).
   - `agent` — judgment-heavy, multi-step, may branch (e.g. `change-safety-review`).
   - `helper` — a precondition/utility other skills call (e.g. `refresh-artifacts`).
3. **The question it answers** in the user's words ("what breaks if…", "where does…").
4. **CLI surface it wraps** — the exact `parrant` command(s) and which JSON/
   Markdown fields it reads. Verify the flags against the current CLI
   (`poetry run parrant --help`), not memory.
5. **Sibling disambiguation** — which existing skill is it most likely confused with,
   and how does its description draw the line?

## Create `skills/dbt-lineage/skills/<slug>/SKILL.md`

Match the house style exactly:

```markdown
---
name: <slug>
description: <what it does>. Use when the user asks "<phrase 1>", "<phrase 2>", or <situation>. <One clause distinguishing it from the closest sibling skill>.
---

# <Title Case Name>

<One or two sentences framing the job and, if relevant, pointing to the sibling
skill for the adjacent job.>

## Inputs
- `<name>` (required/optional, default …).

## Steps
1. Ensure artifacts are fresh (`refresh-artifacts`).   # include for any lineage-reading skill
2. <run the CLI invocation, in a fenced bash block>
3. <read these fields from the output>

## Output
- <the deliverable: what Claude should synthesize, and any ⚠️ flags to raise>
```

Conventions that matter:
- The **`description` is the trigger** — it's how Claude auto-selects the skill. Front-load
  what it does, list concrete user phrasings, and end with the disambiguation clause.
- Lineage-reading skills start with the `refresh-artifacts` precondition (stale artifacts
  are the #1 cause of wrong results).
- Prefer `--format json` for skills that compute; Markdown when the report *is* the deliverable.
- Any external side effect (posting a PR comment, rebuilding artifacts) → confirm with the
  user first.

## Finish

1. Add a row to the **Skills** table in `skills/dbt-lineage/README.md` (`| slug | type | answers |`).
2. Bump `skills/dbt-lineage/.claude-plugin/plugin.json` `version` only if the user
   is cutting a plugin release (ask; don't assume).
3. Show the created file and the README diff, and suggest a `feat(skills): add <slug> skill`
   Conventional Commit — but do not commit unless asked.
