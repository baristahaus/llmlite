# llmlite-proxy on :8300 — what was wrong, what changed, what is verified

Written 2026-10-10 by twoFive (node 25 control), operated for **baristahaus
<baristahaus@local.github.com>** - that identity is the author and committer of both commits
below. Repo: `https://github.com/baristahaus/llmlite`, branch `main`, a fork of
`zouyee/llmlite`. An earlier draft of this file named a different fork URL
(`rui-cruz-git/llmlite`); that was wrong - it returns 404. Checked against the remote config
and the GitHub API on 2026-10-10.

Commits: `8e63209d` (configurable port, env base-URL slots, model aliases) and the commit adding
this file (whole-request reads, map key ownership, loud refusals).

## Pushing: repo-specific deploy key

`origin` uses SSH through a host alias, so this repository uses its own key and nothing else:

```nfs
# ~/.ssh/config
Host github.com-llmlite
    HostName ssh.github.com
    Port 443
    User git
    IdentityFile ~/.ssh/llmlite_deploy_ed25519
    IdentitiesOnly yes
```

```bash
git -C ~/llmlite remote set-url origin git@github.com-llmlite:baristahaus/llmlite.git
```

- Private key: `~/.ssh/llmlite_deploy_ed25519`, mode 600, no passphrase (this node's agent
  pushes without a prompt). Public half: `.pub`, registered as a **Deploy key** on
  `baristahaus/llmlite` with Allow write access.
- Fingerprint: `SHA256:NHbf8W91060W7raf7ttkTRNdzRyKwdeQ4a0It8mp4FQ` (ED25519).
- `IdentitiesOnly yes` matters: without it ssh offers `~/.ssh/id_ed25519` (the cluster key)
  first, and GitHub would authenticate that identity or refuse.
- The alias is deliberately not `github.com`, so other repositories on this machine keep using
  their own credentials.
- GitHub's host keys were taken from `https://api.github.com/meta` (fetched over HTTPS) rather
  than accepted blind from `ssh-keyscan`.
- A deploy key is scoped to this one repository: read/write only here, no account-wide access,
  and revoking it is one click in repo Settings -> Deploy keys.

Deployed binary: `zig-out/bin/llmlite-proxy`, ReleaseSmall, 817,440 bytes, built 19:31.
Start/status/stop: `bash ~/llmlite/setup-8300.sh {start|status|stop}` (log `proxy-8300.log`).
This file lives in the repo on purpose — `/tmp` does not survive a reboot.

## The failure being fixed

Real pi traffic through the proxy failed mid-session with an engine-side parse error, while
direct requests to the same engine succeeded. What the pi client showed:

```
EngineClientError ... 400 {"object":"error","type":"exception","message":"FastJSON:
Unexpected EOF of input data. Index 984 of 1037.", ...}
```

What the proxy log showed for the same class of request (one instance, side by side):

```
info: request: POST /v1/chat/completions ... Content-Length: 23497
info: calling provider openai with body len 16194
warning: HTTP 400 from http://172.30.30.29:8000/v1/chat/completions: {"error":{"message":"1
  validation error:\n {'type': 'json_invalid', 'loc': ('body', 9094), ...
warning: provider openai marked unhealthy
```

A 23,497-byte request went out as 16,194 bytes, and the engine rejected the remnant. The proxy
then marked the backend unhealthy, so the next requests for either model name went to the other
slot: a 117-byte request that had worked before the failures returned 502.

## Five defects

**1 — Requests were read in pieces, so long prompts were truncated.**
`handleConnection` read into a 16 KiB header buffer, a 1 MiB `head`, a 16 KiB `body_buffer`,
and a 1 MiB `full_request`, and returned on whichever read happened to complete. Worse,
header parsing looped to the **first** `\n` and returned that one line as the whole header
block, so `Content-Length` and `Transfer-Encoding` were effectively never seen. Any request
beyond the first segment was routed on ~1.5 KiB of buffered bytes and forwarded truncated —
that is the `984 of 1037`: the first read, cut at a buffer boundary. An agent request carries
system prompt, tool schemas and tool results, so it goes over 16 KiB immediately: every real
turn failed, small test probes passed.

Now: read until headers complete, parse them, take `Content-Length`, allocate exactly that
many bytes, read until satisfied or EOF, then route and forward.

**2 — A body over the 1 MiB cap was dropped entirely.** `body_fits` decided whether to read
at all; 4 MiB of body was replaced with the ~1.5 KiB header block and sent upstream, or read
partially and forwarded short. Fixed by the same rewrite — size the buffer from the header,
never guess. The cap becomes `LLMLITE_MAX_REQUEST_BYTES`, default 64 MiB, and is enforced
**before** allocation: over it → 413 naming the limit and the configured variable. No silent
anything.

**3 — `Transfer-Encoding: chunked` was ignored and the request forwarded as body-less.**
The matcher compared 17 characters against `"transfer-encoding:"`, which is 18, so chunked
requests went undetected, were treated as `Content-Length: 0`, and the backend got an empty
body (observed as provider 502). There is no way to answer a chunked body with `Content-Length`
semantics without buffering it in pieces, and partial forwarding produced exactly the corruption
this change exists to remove. So such requests, and POSTs with neither `Content-Length` nor
`Transfer-Encoding`, are refused with **411**, naming the unsupported form. curl's
`--data-binary` and the pi SDK both send `Content-Length`, so this path is rare — but when it
happens the client sees a refusal, not a corrupt answer.

**4 — The rate limiter stored a borrowed key, so every fresh proxy died on request two.**
After fix 1, each newly started proxy segfaulted on its **second** request. A safety-checked
Debug build put the fault in `rate_limit.zig:40`, inside `array_hash_map.getOrPutContext`,
reached from the rate-limit check in `handleChatCompletions`. `RateLimiter.check` passed the
caller's `api_key` slice — a view into the request buffer — straight to `windows.getOrPut`.
That map owns its keys (its own `deinit` frees them), so it stored a pointer into memory the
connection had already released; the next request hashed a dead key. The old stack array hid
it: the stored address stayed mapped and was overwritten by the next connection, giving
silently wrong rate-limit accounting instead of a crash. Moving the request to the heap made
it fatal. `check` now inserts only when absent and gives the map its own copy.

Breaker settings as found in config (not invented here): `failure_threshold 4`,
`recovery_timeout` 60 s, half-open after 2 successes. Because every agentic turn failed on the
16 KiB ceiling, `openai` was marked unhealthy again as soon as it got a chance, so named-backend
traffic stayed diverted. `LLMLITE_CIRCUIT_BREAKER=0` refuses the trip entirely; with it set, two
deliberate backend failures returned two honest 502s and the next request reached its named
backend (200), and `circuit breaker open` stayed at the 3 lines written before the switch existed.
Set `LLMLITE_CIRCUIT_BREAKER=1` for upstream behaviour.

Same ownership class in `loadAliases` (added in the earlier commit): alias keys pointed into
the config buffer, which the map would free as keys. Keys are copied there now; values still
point into that buffer, which outlives the map.

Left alone and known: `ActiveHealthChecker` keys `probe_state` with `provider.toString()`, a
comptime literal, and `StringArrayHashMap.deinit` frees keys — an invalid free at shutdown
only. Deliberately untouched, to keep this diff to what a live deployment needs.

**5 — Pretty-printed JSON skipped model normalisation.** `normalizeModelInBody` searched for
`"model":"` with no space, so a client sending `"model": "glm53f.llite"` (any `json.dumps`
body) was routed by its alias but forwarded the alias verbatim, and the backend answered 404
"The model `glm53f.llite` does not exist". `extractModelFromBody` already accepted both
spacings, so routing worked while forwarding did not. Routing and forwarding now share
`headerValue`-style whitespace-tolerant scanning.

## Verified

Every row below is one request through the deployed ReleaseSmall binary on :8300 against the
engine on node 29, in an agentic shape (system prompt + six tool schemas + a tool result,
`max_tokens=64`) unless noted. Pass = HTTP 200 with non-empty content. These are single
requests per size, not averaged rates: I did not run repeated trials, so no pass *rates* are
claimed.

| request through the proxy | after (measured) | before |
|---|---|---|
| 103–112 B, each of the four model names | 4/4 HTTP 200 with content | small probes did work, until the breaker latched on the next failure |
| 118 B pretty-printed, 132 B `indent=1` | 2/2 HTTP 200 | 404 "model does not exist" (defect 5) |
| 4,319 B and 28,119 B | 2/2 HTTP 200 with content | observed: 23,497 B request forwarded as 16,194 B, engine 400 `json_invalid` at body 9094; a second instance failed at body 84 |
| 168,119 B and 560,119 B | 2/2 HTTP 200 with content | not measured: the old code capped the body at 1 MiB and the whole request at 16 KiB, so these could not be read at all |
| 62,353 B agentic shape (system + 6 tools + tool result) | 200, returned a `tool_call` | same truncation class as above |
| 12 mixed requests up to 21 KB, 2 concurrent, production flags | 12/12 200, no faults, on safety-checked Debug and deployed ReleaseSmall | after defect 1 was fixed but before defect 4 was: died on request 2 of every start |
| `Transfer-Encoding: chunked` | 411 naming the form | provider 502, because it was forwarded with an empty body |
| 6,097 B with `LLMLITE_MAX_REQUEST_BYTES=4096` | 413; log line `refusing request: 189 header + 6097 body bytes exceeds the 4096 byte ceiling`; the next small request on the same instance was served (200) | n/a, the ceiling is new |
| POST with no body and no `Content-Length` | 411 | forwarded as a body-less request |

Both sides of the wire were checked. Before: the engine log on node 29 showed `json_invalid` at
body offsets 9094 and 84 for requests the proxy had shortened, and the proxy log for the same
request pair shows `Content-Length: 23497` next to `calling provider openai with body len 16194`.
After: `docker logs --since 25m glm53f-r0 | grep -cE 'json_invalid|FastJSON'` = 0.

Rows marked "not measured" are inferred from the old code path, not observed; the 1 KiB and
20 KiB rows are single requests per size, not trials, so no pass rates are claimed anywhere.

End to end through pi itself, using the existing `~/.pi/agent/models.json` entry:

```bash
pi --model glm53f.llite --thinking off -p "Read the first 3 lines of .../NOTES-8300.md, then reply PROXY-OK-1161"
```

It answered `PROXY-OK-1161` after a real tool call. The proxy logged two forwarded bodies,
13,647 and 14,126 bytes - each one larger than the entire 16 KiB request window the old code
had, headers included - with no `json_invalid` and no breaker activity.

End to end through pi itself, using the entry already in `~/.pi/agent/models.json`:

```bash
pi --model glm53f.llite --thinking off -p "Read the first 3 lines of .../NOTES-8300.md, then reply with exactly: PROXY-OK-1161"
```

It answered `PROXY-OK-1161` after a real read-tool call. The proxy logged two forwarded bodies,
13,647 and 14,126 bytes - each larger than the entire 16 KiB request window the old code had,
headers included - with no `json_invalid` and no breaker activity.

## Still limited, on purpose

- Chunked request bodies: refused 411 (defect 3).
- Requests with no `Content-Length`: refused 411 rather than forwarded body-less.
- Keep-alive is still answered with `Connection: close`.
- Idle connections can still log `StreamTimeout` at 30s; harmless, noisy.
- Shutdown-only invalid free in `ActiveHealthChecker` (see defect 4 note).

## Files

| file | change |
|---|---|
| `src/proxy/server.zig` | `readRequest` (whole-request, 413/411 paths), `headerValue`, `containsChunked`, whitespace-tolerant `normalizeModelInBody`, alias key ownership, `circuit_breaker_enabled` and its two gates |
| `src/proxy/rate_limit.zig` | `check` inserts only when absent, with an owned key copy |
| `src/proxy/config.zig` | `max_request_bytes`, `circuit_breaker_enabled` from env |
| `src/proxy_main.zig` | `--rate-limit`; startup log of ceiling and breaker state |
| `setup-8300.sh` | ceiling/breaker env, status shows both |
| `aliases.ini` | the four names currently served |

Old sources kept beside the tree: `server.zig.bak-20261010-sizefix` (pre-request-reader),
`aliases.ini.bak-20261010-llite`.

## One commit, no push

```bash
cd ~/llmlite
git -c user.name="twoFive (node 25 control)" -c user.email="twoFive@cluster.local" \
    commit -am "Read whole requests before routing; own map keys; refuse loud, never corrupt"
```
