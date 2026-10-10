
## Pushed, and under whose name

Pushed 2026-10-10 over the deploy key above:

```bash
git -C ~/llmlite push -u origin main     # e5ffb45f..69650fd0  main -> main
```

Both commits are authored and committed as `baristahaus <baristahaus@local.github.com>`, so
GitHub credits the fork owner. The re-authoring happened **before** the first push, so no
published history was rewritten; the code files were confirmed byte-identical afterwards
(`git diff` against the pre-rewrite tree showed only `NOTES-8300.md`).

Verified on GitHub's side after the push: `main` = `69650fd0`, author and committer both
`baristahaus@local.github.com`, and `ssh -T` against the alias reports
`Hi baristahaus/llmlite!` - i.e. the key is scoped to this repository alone.

To undo the whole setup later: delete the deploy key in repo Settings -> Deploy keys, remove
`~/.ssh/llmlite_deploy_ed25519*`, drop the `Host github.com-llmlite` block from
`~/.ssh/config`, and point `origin` back at the HTTPS URL.
