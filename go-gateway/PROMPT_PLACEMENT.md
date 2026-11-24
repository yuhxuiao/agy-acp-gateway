# Opt-in first-prompt placement

`AGY_PROMPT_PLACEMENT` selects how generated client instructions enter ACP:

| Value | Behavior |
| --- | --- |
| unset or `append` | Existing native `session/new` system-prompt append path (default). |
| `first-prompt` | Remove only `_meta.antigravity.systemPrompt.append` from session creation and prepend its text to the first prompt. |

Only the exact value `first-prompt` enables compatibility mode. Other values retain the default path. Configuration is process-wide: **restart the gateway when changing modes**. Hot switching an existing engine/session is not supported.

## Semantics and tradeoffs

Compatibility mode places the generated append text after a `# Pi session instructions` heading and before a `# Current Pi user request` separator. Original prompt blocks follow unchanged, including media blocks. Opaque metadata siblings and model/thought-level configuration ACKs remain unchanged.

This intentionally changes instruction role/priority and may change upstream caching behavior. It is **not equivalent to system-role instructions**, not a lossless transformation of the complete incoming request, and not a workaround for policy restrictions. Normal gateway prompt construction and sanitation still occur before placement. No upstream backend/policy root cause or universal success guarantee is established by this option.

Placement state is isolated by ACP session and engine generation. The append text is injected at most once into a new session. A failed first dispatch has uncertain delivery, so its provenance is invalidated rather than re-armed. A later client request uses the normal gateway fresh-session/full-host-history recovery path. Unknown or retired recovery provenance similarly falls back to fresh history; instructions are not silently omitted or blindly injected into an existing upstream conversation.

Close and generation retirement release placement state. Cancel and state controls remain native. Existing completed-result reuse, detached completion, compaction-target ownership and cleanup remain separate from inference resubmission.

## Retry and recovery policy

In `first-prompt` mode, the gateway does not automatically resubmit failed prompt RPCs, run WAF probe inference, replay for WAF recovery, or replay for capacity/degradation fallback. Normal tool continuation and already-completed-result reuse are not inference retries and remain available. This does not establish that the official CLI never retries internally.

## Native credential access

The default mode retains the existing child D-Bus masking policy. Compatibility mode instead inherits the gateway process's user D-Bus connection. On Linux this allows the child to access an already-authorized native Secret Service login.

**Security scope:** the child gains access to that user bus and its exposed services. Enable the mode only in a trusted user/service environment with the intended existing identity. This option does not create, copy or import credentials, change login/profile selection, perform login, or accept new terms. Existing profile paths and authentication configuration must already be correct; users should review their service environment explicitly.

## Validation scope

The upstream port is validated offline against the fake ACP harness and native RPC boundary, covering default passthrough, exact append-leaf transfer, state ACKs, media blocks, cancellation, failure/restart history recovery, generation cleanup, and current compaction/detached ownership paths. The full default-mode suite is also retained.

A prior version-based private build was tested with a real Neon LOW client session, tools and reload/history recovery. That observation is not live validation of this upstream revision, other models, accounts or environments.

Metadata logs (`PLACEMENT_HTTP`, `PLACEMENT_NEW`, `PLACEMENT_PROMPT`) expose byte counts, hashes and stage counts, not raw prompt text. These metadata fingerprints may still be sensitive; handle gateway logs accordingly.
